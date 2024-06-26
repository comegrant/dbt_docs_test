import logging

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
logger = logging.getLogger(__name__)


def set_primary_keys_table(full_table_name: str, primary_keys_cols: list) -> None:
    constraint_statement = ", ".join(primary_keys_cols)
    pk_name = "pk_" + full_table_name.replace(".", "_")

    # Set primary keys columns non nullable
    for key in primary_keys_cols:
        spark.sql(f"ALTER TABLE {full_table_name} ALTER column {key} SET NOT NULL")

    # Check if primary key constraint already exists
    spark.sql(f"ALTER TABLE {full_table_name} DROP CONSTRAINT IF EXISTS {pk_name}")

    spark.sql(
        f"ALTER TABLE {full_table_name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({constraint_statement})"
    )

    logger.info(f"Primary keys set for table {full_table_name}")
