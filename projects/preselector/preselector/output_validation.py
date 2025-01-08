import datetime as dt

import polars as pl
from data_contracts.preselector.store import Preselector, SuccessfulPreselectorOutput
from data_contracts.sources import databricks_catalog

COMPLIANCY_ALLERGEN = 1
COMPLIANCY_PREFERENCE = 2
ERROR_AVG = 0.002
ERROR_ACC = 0.02
ERROR_MEAN_ORDERED_AGO = 0.02


async def get_output_data(start_yyyyww: int | None = None, output_type: str = "batch") -> pl.DataFrame:
    if start_yyyyww is None:
        start_date = dt.datetime.now(tz=dt.timezone.utc) + dt.timedelta(weeks=5)
        start_yyyyww = int(f"{start_date.year}{start_date.isocalendar()[1]:02d}")

    if output_type == "batch":
        df = (
            await Preselector.query()
            .using_source(databricks_catalog.schema("mloutputs").table("preselector_batch"))
            .select(
                {
                    "company_id",
                    "year",
                    "week",
                    "agreement_id",
                    "portion_size",
                    "compliancy",
                    "error_vector",
                }
            )
            .filter(pl.col("year") * 100 + pl.col("week") >= start_yyyyww)
            .rename({"agreement_id": "billing_agreement_id", "year": "menu_year", "week": "menu_week"})
            .to_polars()
        )

    elif output_type == "realtime":
        df = (
            await SuccessfulPreselectorOutput.query()
            .filter(pl.col("menu_year") * 100 + pl.col("menu_week") >= start_yyyyww)
            .unique_entities()
            .to_polars()
        ).select(
            [
                "company_id",
                "menu_year",
                "menu_week",
                "billing_agreement_id",
                "portion_size",
                "compliancy",
                "error_vector",
            ]
        )

    else:
        raise ValueError(f"Invalid output type: {output_type}")

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


def validation_summary(df_compliancy: pl.DataFrame, df_error_vector: pl.DataFrame) -> str:
    df = df_compliancy.join(df_error_vector, on=["company_id", "portion_size"], how="left", coalesce=True)
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
            )
            errors_compliance = (
                row["agreement_id_broken_allergen"][0][:10]
                if isinstance(row["agreement_id_broken_allergen"], list)
                else row["agreement_id_broken_allergen"]
            )
            warnings_compliancy = (
                row["agreement_id_broken_preference"][0][:10]
                if isinstance(row["agreement_id_broken_preference"], list)
                else row["agreement_id_broken_preference"]
            )

            error_dim = (
                row["agreement_id_mean_ordered_ago"][0][:10]
                if isinstance(row["agreement_id_mean_ordered_ago"], list)
                else row["agreement_id_mean_ordered_ago"]
            )

            warnings_dim_1 = (
                row["agreement_id_avg_error"][0][:10]
                if isinstance(row["agreement_id_avg_error"], list)
                else row["agreement_id_avg_error"]
            )

            warnings_dim_2 = (
                row["agreement_id_acc_error"][0][:10]
                if isinstance(row["agreement_id_acc_error"], list)
                else row["agreement_id_acc_error"]
            )

            group_str += "     ---\n"
            group_str += f"     Agreement ids with broken allergens: {errors_compliance}\n"
            group_str += f"     Agreement ids with broken preferences: {warnings_compliancy}\n"
            group_str += "     ---\n"
            group_str += f"     Agreement ids with error mean ordered ago: {error_dim}\n"
            group_str += f"     Agreement ids with warning avg. error: {warnings_dim_1}\n"
            group_str += f"     Agreement ids with warning acc. error: {warnings_dim_2}\n"
        return group_str

    formatted_table = "\n".join(
        format_group(group_df, str(company_id))
        for company_id, group_df in df.group_by("company_id", maintain_order=True)
    )

    return formatted_table


def validation_metrics(df_compliancy: pl.DataFrame, df_error_vector: pl.DataFrame) -> dict[str, int]:
    df = df_compliancy.join(df_error_vector, on=["company_id", "portion_size"], how="left", coalesce=True)

    metrics = {
        "total_checks": int(df.height),
        "comliancy_warnings": int(df["compliancy_warning"].sum()),
        "vector_warnings": int(df["vector_warning"].sum()),
        "comliancy_errors": int(df["compliancy_error"].sum()),
        "vector_errors": int(df["vector_error"].sum()),
    }

    return metrics
