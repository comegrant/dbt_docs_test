import datetime as dt

import polars as pl
from data_contracts.preselector.store import Preselector, SuccessfulPreselectorOutput
from data_contracts.sources import databricks_catalog

COMPLIANCY_ALLERGEN = 1
COMPLIANCY_PREFERENCE = 2


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
                    "generated_at",
                    "model_version",
                }
            )
            .filter(pl.col("menu_year") * 100 + pl.col("menu_week") >= start_yyyyww)
            .rename({"agreement_id": "billing_agreement_id", "year": "menu_year", "week": "menu_week"})
            .to_polars()
        )

    elif output_type == "realtime":
        df = (
            await SuccessfulPreselectorOutput.query()
            .select(
                {
                    "company_id",
                    "menu_year",
                    "menu_week",
                    "billing_agreement_id",
                    "portion_size",
                    "compliancy",
                    "generated_at",
                    "model_version",
                }
            )
            .filter(pl.col("menu_year") * 100 + pl.col("menu_week") >= start_yyyyww)
            .unique_entities()
            .to_polars()
        )

    else:
        raise ValueError(f"Invalid output type: {output_type}")

    return df


def compliancy_metrics(df: pl.DataFrame) -> pl.DataFrame:
    groupby_cols = ["company_id", "portion_size"]

    result = df.group_by(groupby_cols).agg(
        [
            pl.len().alias("total_records"),
            pl.col("compliancy").filter(pl.col("compliancy") == COMPLIANCY_ALLERGEN).count().alias("broken_allergen"),
            pl.col("compliancy")
            .filter(pl.col("compliancy") == COMPLIANCY_PREFERENCE)
            .count()
            .alias("broken_preference"),
            (pl.col("compliancy").filter(pl.col("compliancy") == COMPLIANCY_ALLERGEN).count() / pl.len() * 100)
            .round(2)
            .alias("percentage_allergen"),
            (pl.col("compliancy").filter(pl.col("compliancy") == COMPLIANCY_PREFERENCE).count() / pl.len() * 100)
            .round(2)
            .alias("percentage_preference"),
            (pl.col("compliancy") == COMPLIANCY_ALLERGEN).any().alias("error"),
            (pl.col("compliancy") == COMPLIANCY_PREFERENCE).any().alias("warning"),
            pl.col("billing_agreement_id")
            .filter(pl.col("compliancy") == COMPLIANCY_ALLERGEN)
            .implode()
            .alias("agreement_id_errors"),
            pl.col("billing_agreement_id")
            .filter(pl.col("compliancy") == COMPLIANCY_PREFERENCE)
            .implode()
            .alias("agreement_id_warnings"),
        ]
    )

    return result.sort(["company_id", "portion_size"], descending=[True, True])


def validation_summary(df_compliancy: pl.DataFrame) -> str:
    df = df_compliancy.sort(["company_id", "portion_size"])

    def format_group(group: pl.DataFrame, group_name: str) -> str:
        group_str = f"{group_name}\n"
        for row in group.iter_rows(named=True):
            group_str += f"  -- Portion Size {row['portion_size']}:\n"
            group_str += (
                f"     Total Records: {row['total_records']}, "
                f"Broken Allergens: {row['broken_allergen']} "
                f"({row['percentage_allergen']}%), "
                f"Broken Preferences: {row['broken_preference']} "
                f"({row['percentage_preference']}%)\n"
            )
            errors = (
                row["agreement_id_errors"][0][:10]
                if isinstance(row["agreement_id_errors"], list)
                else row["agreement_id_errors"]
            )
            warnings = (
                row["agreement_id_warnings"][0][:10]
                if isinstance(row["agreement_id_warnings"], list)
                else row["agreement_id_warnings"]
            )
            group_str += f"     Agreement ids with errors: {errors}\n"
            group_str += f"     Agreement ids with warnings: {warnings}\n"
        return group_str

    formatted_table = "\n".join(
        format_group(group_df, str(company_id))
        for company_id, group_df in df.group_by("company_id", maintain_order=True)
    )

    return formatted_table


def validation_metrics(df_compliancy: pl.DataFrame) -> tuple[int, int, int]:
    total_compliancy_checks = int(df_compliancy.height)
    comliancy_warnings = int(df_compliancy["warning"].sum())
    comliancy_errors = int(df_compliancy["error"].sum())

    return total_compliancy_checks, comliancy_warnings, comliancy_errors
