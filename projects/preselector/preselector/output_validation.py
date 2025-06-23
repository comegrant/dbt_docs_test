import datetime as dt
from typing import Optional

import polars as pl
from aligned import ContractStore
from constants.companies import get_company_by_code
from data_contracts.preselector.store import Preselector, SuccessfulPreselectorOutput
from data_contracts.recipe import RecipeMainIngredientCategory
from data_contracts.sources import databricks_catalog

COMPLIANCY_ALLERGEN = 1
COMPLIANCY_PREFERENCE = 2
ERROR_AVG = 0.003
ERROR_ACC = 0.12
ERROR_MEAN_ORDERED_AGO = 0.02
VAR_THRESHOLD = 0.59


async def get_output_data(
    start_yyyyww: int | None = None,
    output_type: str = "batch",
    company: str = "GL",
    model_version: str | None = None,
) -> pl.DataFrame:
    """
    Returns the output that should be validated.
    """
    if start_yyyyww is None:
        start_date = dt.datetime.now(tz=dt.timezone.utc) + dt.timedelta(weeks=4)
        start_yyyyww = int(f"{start_date.year}{start_date.isocalendar()[1]:02d}")

    company_id = get_company_by_code(company).company_id

    if output_type == "realtime":
        df = (
            await SuccessfulPreselectorOutput.query()
            .filter(pl.col("menu_year").cast(pl.Int32) * 100 + pl.col("menu_week") == start_yyyyww)
            .filter(pl.col("company_id") == company_id)
            .unique_entities()
            .to_polars()
        ).select(
            [
                "company_id",
                "menu_year",
                "menu_week",
                "billing_agreement_id",
                "main_recipe_ids",
                "portion_size",
                "number_of_recipes",
                "compliancy",
                "error_vector",
                "model_version",
            ]
        )
    else:
        source_splits = output_type.split(".")

        if len(source_splits) == 2:  # noqa: PLR2004
            schema, table = source_splits
            source = databricks_catalog.schema(schema).table(table)
        elif output_type == "batch":
            source = databricks_catalog.schema("mloutputs").table("preselector_batch")
        else:
            raise ValueError(f"Unsupported output type: {output_type}")

        df = (
            await Preselector.query()
            .using_source(source)
            .select(
                {
                    "company_id",
                    "year",
                    "week",
                    "agreement_id",
                    "portion_size",
                    "compliancy",
                    "error_vector",
                    "main_recipe_ids",
                    "model_version",
                }
            )
            .all()
            .filter(pl.col("year").cast(pl.Int32) * 100 + pl.col("week") == start_yyyyww)
            .filter(pl.col("company_id") == company_id)
            .rename({"agreement_id": "billing_agreement_id", "year": "menu_year", "week": "menu_week"})
            .to_polars()
        ).with_columns(number_of_recipes=pl.col("main_recipe_ids").list.len())

    if model_version is not None:
        df = df.filter(pl.col("model_version") == model_version)

    return df


def count_over_compliance(compliance: int) -> pl.Expr:
    return pl.col("compliancy").filter(pl.col("compliancy") == compliance).count()


def compliancy_metrics(df: pl.DataFrame) -> pl.DataFrame:
    groupby_cols = ["company_id", "portion_size"]

    result = df.group_by(groupby_cols).agg(
        [
            pl.len().alias("total_records"),
            count_over_compliance(COMPLIANCY_ALLERGEN).alias("broken_allergen"),
            count_over_compliance(COMPLIANCY_PREFERENCE).alias("broken_preference").alias("broken_preference"),
            (count_over_compliance(COMPLIANCY_ALLERGEN) / pl.len() * 100).round(2).alias("percentage_allergen"),
            (count_over_compliance(COMPLIANCY_PREFERENCE).alias("broken_preference") / pl.len() * 100)
            .round(2)
            .alias("percentage_preference"),
            (pl.col("compliancy") == COMPLIANCY_ALLERGEN).any().alias("compliancy_error"),
            (pl.col("compliancy") == COMPLIANCY_PREFERENCE).any().alias("compliancy_warning"),
            pl.col("billing_agreement_id")
            .filter(pl.col("compliancy") == COMPLIANCY_ALLERGEN)
            .implode()
            .alias("agreement_id_broken_allergen"),
            pl.col("billing_agreement_id")
            .filter(pl.col("compliancy") == COMPLIANCY_PREFERENCE)
            .implode()
            .alias("agreement_id_broken_preference"),
        ]
    )

    return result.sort(["company_id", "portion_size"], descending=[True, True])


def count_over_threshold(threshold: float, column: str) -> pl.Expr:
    return pl.col(column).filter(pl.col(column) > threshold).count()


def error_metrics(df: pl.DataFrame) -> pl.DataFrame:
    err_explode = df.select("company_id", "portion_size", "billing_agreement_id", "error_vector").unnest("error_vector")

    exclude_cols = ["company_id", "portion_size", "billing_agreement_id"]
    error_cols = [col for col in err_explode.columns if col not in exclude_cols]

    err_explode = err_explode.with_columns(
        (pl.mean_horizontal(error_cols).alias("avg_error")),
        (pl.sum_horizontal(error_cols).alias("acc_error")),
    )

    results = err_explode.group_by(["company_id", "portion_size"]).agg(
        [
            pl.len().alias("total_records"),
            count_over_threshold(ERROR_MEAN_ORDERED_AGO, "mean_ordered_ago").alias("broken_mean_ordered_ago"),
            (count_over_threshold(ERROR_MEAN_ORDERED_AGO, "mean_ordered_ago") / pl.len() * 100)
            .round(2)
            .alias("percentage_mean_ordered_ago"),
            count_over_threshold(ERROR_AVG, "avg_error").alias("broken_avg_error"),
            (count_over_threshold(ERROR_AVG, "avg_error") / pl.len() * 100).round(2).alias("percentage_avg_error"),
            count_over_threshold(ERROR_ACC, "acc_error").alias("broken_acc_error"),
            (count_over_threshold(ERROR_ACC, "acc_error") / pl.len() * 100).round(2).alias("percentage_acc_error"),
            (pl.col("mean_ordered_ago") >= ERROR_MEAN_ORDERED_AGO).any().alias("vector_error"),
            ((pl.col("avg_error") > ERROR_AVG) | (pl.col("acc_error") > ERROR_ACC)).any().alias("vector_warning"),
            pl.col("billing_agreement_id")
            .filter(pl.col("mean_ordered_ago") > ERROR_MEAN_ORDERED_AGO)
            .implode()
            .alias("agreement_id_mean_ordered_ago"),
            pl.col("billing_agreement_id")
            .filter(pl.col("avg_error") > ERROR_AVG)
            .implode()
            .alias("agreement_id_avg_error"),
            pl.col("billing_agreement_id")
            .filter(pl.col("acc_error") > ERROR_AVG)
            .implode()
            .alias("agreement_id_acc_error"),
        ]
    )

    return results.sort(["company_id", "portion_size"], descending=[True, True])


def count_max_repetition(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .list.eval(pl.element().value_counts())
        .list.eval(pl.element().struct.field("count").max())
        .list.first()
    )


def calculate_repetition_ratio(column: str) -> pl.Expr:
    return (pl.col(column) / pl.col("number_of_recipes").cast(pl.Float32)) > VAR_THRESHOLD


async def add_variation_warning(df: pl.DataFrame, dummy: Optional[ContractStore] = None) -> pl.DataFrame:
    all_recipe_ids = (
        df.select(
            ["billing_agreement_id", "menu_week", "menu_year", "portion_size", "number_of_recipes", "main_recipe_ids"]
        )
        .explode("main_recipe_ids")
        .rename({"main_recipe_ids": "recipe_id"})
    )

    store = dummy.feature_view(RecipeMainIngredientCategory) if dummy else RecipeMainIngredientCategory.query()
    with_main_ingredient = await store.features_for(all_recipe_ids).to_polars()

    grouped = with_main_ingredient.group_by(
        ["billing_agreement_id", "menu_week", "menu_year", "portion_size", "number_of_recipes"]
    ).agg(
        [
            pl.col("recipe_id").alias("main_recipe_ids"),
            pl.col("main_protein_category_id").alias("main_protein_ids"),
            pl.col("main_carbohydrate_category_id").alias("main_carb_ids"),
        ]
    )

    df_final = (
        df.join(
            grouped,
            on=["billing_agreement_id", "menu_week", "menu_year", "portion_size", "number_of_recipes"],
            how="left",
            suffix="_1",
        )
        .with_columns(
            [
                count_max_repetition("main_carb_ids").alias("max_carb_repetition"),
                count_max_repetition("main_protein_ids").alias("max_protein_repetition"),
            ]
        )
        .with_columns(
            [
                calculate_repetition_ratio("max_carb_repetition").alias("variation_warning_carb"),
                calculate_repetition_ratio("max_protein_repetition").alias("variation_warning_protein"),
            ]
        )
    )

    return df_final


async def variation_metrics(df: pl.DataFrame, dummy: Optional[ContractStore] = None) -> pl.DataFrame:
    df = await add_variation_warning(df, dummy)
    groupby_cols = ["company_id", "portion_size"]

    result = df.group_by(groupby_cols).agg(
        [
            pl.len().alias("total_records"),
            pl.col("variation_warning_carb").sum().alias("carb_warnings"),
            pl.col("variation_warning_protein").sum().alias("protein_warnings"),
            (pl.col("variation_warning_carb").sum() / pl.len() * 100).round(2).alias("percentage_carb_warnings"),
            (pl.col("variation_warning_protein").sum() / pl.len() * 100).round(2).alias("percentage_protein_warnings"),
            pl.col("variation_warning_carb").any().alias("has_carb_warning"),
            pl.col("variation_warning_protein").any().alias("has_protein_warning"),
            pl.col("billing_agreement_id")
            .filter(pl.col("variation_warning_carb"))
            .implode()
            .alias("agreement_id_carb_warnings"),
            pl.col("billing_agreement_id")
            .filter(pl.col("variation_warning_protein"))
            .implode()
            .alias("agreement_id_protein_warnings"),
        ]
    )

    return result.sort(["company_id", "portion_size"], descending=[True, True])


def extract_first_10(value: list | str) -> list[str]:
    if isinstance(value, list):
        return [str(x) for x in value[:10]]
    return [str(value)]


def validation_summary(df_compliancy: pl.DataFrame, df_error_vector: pl.DataFrame, df_variation: pl.DataFrame) -> str:
    df = df_compliancy.join(df_error_vector, on=["company_id", "portion_size"], how="left", suffix="_1", coalesce=True)
    df = df.join(df_variation, on=["company_id", "portion_size"], how="left", suffix="_2", coalesce=True)
    df = df.sort(["company_id", "portion_size"])

    def format_group(group: pl.DataFrame, group_name: str) -> str:
        group_str = f"{group_name}\n"
        for row in group.iter_rows(named=True):
            group_str += f"  -- Portion Size {row['portion_size']}:\n"
            group_str += (
                f"     Total Records: {row['total_records']}, \n"
                f"     Broken Allergens: {row['broken_allergen']} "
                f"({row['percentage_allergen']}%), \n"
                f"     Broken Preferences: {row['broken_preference']} "
                f"({row['percentage_preference']}%)\n"
                f"     Mean ordered ago error: {row['broken_mean_ordered_ago']} "
                f"({row['percentage_mean_ordered_ago']}%)\n"
                f"     Avg. error warning: {row['broken_avg_error']} "
                f"({row['percentage_avg_error']}%)\n"
                f"     Acc. error warning: {row['broken_acc_error']} "
                f"({row['percentage_acc_error']}%)\n"
                f"     Broken carb variation: {row['carb_warnings']} "
                f"({row['percentage_carb_warnings']}%)\n"
                f"     Broken protein variation: {row['protein_warnings']} "
                f"({row['percentage_protein_warnings']}%)\n"
            )
            errors_compliance = extract_first_10(row["agreement_id_broken_allergen"])
            warnings_compliancy = extract_first_10(row["agreement_id_broken_preference"])

            error_dim = extract_first_10(row["agreement_id_mean_ordered_ago"])

            warnings_dim_1 = extract_first_10(row["agreement_id_avg_error"])

            warnings_dim_2 = extract_first_10(row["agreement_id_acc_error"])

            carb_warnings = extract_first_10(row["agreement_id_carb_warnings"])

            protein_warnings = extract_first_10(row["agreement_id_protein_warnings"])

            group_str += "     ---\n"
            group_str += f"     Agreement ids with broken allergens: {errors_compliance}\n"
            group_str += f"     Agreement ids with broken preferences: {warnings_compliancy}\n"
            group_str += "     ---\n"
            group_str += f"     Agreement ids with error mean ordered ago: {error_dim}\n"
            group_str += f"     Agreement ids with warning avg. error: {warnings_dim_1}\n"
            group_str += f"     Agreement ids with warning acc. error: {warnings_dim_2}\n"
            group_str += f"     Agreement ids with carb variation warning: {carb_warnings}\n"
            group_str += f"     Agreement ids with protein variation warning: {protein_warnings}\n"
        return group_str

    formatted_table = "\n".join(
        format_group(group_df, str(company_id))
        for company_id, group_df in df.group_by("company_id", maintain_order=True)
    )

    return formatted_table


def calc_percentage(value: int | float, total: int | float) -> float:
    return round((value / total) * 100, 2)


def validation_metrics(
    df_compliancy: pl.DataFrame, df_error_vector: pl.DataFrame, df_variation: pl.DataFrame
) -> dict[str, int]:
    df = df_compliancy.join(df_error_vector, on=["company_id", "portion_size"], how="left", suffix="_1", coalesce=True)
    df = df.join(df_variation, on=["company_id", "portion_size"], how="left", suffix="_2", coalesce=True)

    total_records = df["total_records"].sum()

    metrics = {
        "sum_total_records": int(total_records),
        "perc_broken_allergen": calc_percentage(df["broken_allergen"].sum(), total_records),
        "perc_broken_preference": calc_percentage(df["broken_preference"].sum(), total_records),
        "perc_broken_mean_ordered_ago": calc_percentage(df["broken_mean_ordered_ago"].sum(), total_records),
        "perc_broken_avg_error": calc_percentage(df["broken_avg_error"].sum(), total_records),
        "perc_broken_acc_error": calc_percentage(df["broken_acc_error"].sum(), total_records),
        "perc_carb_warnings": calc_percentage(df["carb_warnings"].sum(), total_records),
        "perc_protein_warnings": calc_percentage(df["protein_warnings"].sum(), total_records),
    }

    return metrics
