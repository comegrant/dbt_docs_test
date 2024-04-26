import os

import pandas as pd
import pytest
from data_connector.databricks_connector import save_dataframe_to_table
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    spark = (
        SparkSession.builder.appName("Spark test session")
        .master("local[1]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.test_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    yield spark
    spark.stop()


def test_single_space(spark: SparkSession) -> None:
    # Apply the transformation function from before
    transformed_data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 28},
    ]
    transformed_df = spark.createDataFrame(transformed_data)

    expected_data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 28},
    ]

    expected_df = spark.createDataFrame(expected_data)

    assertDataFrameEqual(transformed_df, expected_df)


def test_write_to_table(spark: SparkSession) -> None:
    data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 28},
    ]
    df = spark.createDataFrame(data)
    df.write.mode("overwrite").saveAsTable("source_table")

    df = spark.sql("SELECT * FROM source_table")
    df.write.mode("overwrite").saveAsTable("destination_table")

    # Verify by reading back
    result_df = spark.table("destination_table")
    assert df.sort("age").collect() == result_df.sort("age").collect()

    # Clean up
    spark.sql("DROP TABLE IF EXISTS source_table")
    spark.sql("DROP TABLE IF EXISTS destination_table")


def test_save_dataframe_to_table(spark: SparkSession) -> None:
    # Apply the transformation function from before
    data = pd.DataFrame(
        [
            {"name": "John D.", "age": 30},
            {"name": "Alice G.", "age": 25},
            {"name": "Bob T.", "age": 35},
            {"name": "Eve A.", "age": 28},
        ],
    )

    # Create test schema
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

    save_dataframe_to_table(
        data,
        table_name="dataframe_table",
        schema="test_schema",
        mode="overwrite",
    )

    # Read back the table
    result = spark.sql("SELECT * FROM test_schema.dataframe_table").toPandas()

    assert result.equals(data)

    # Clean up
    spark.sql("DROP TABLE IF EXISTS test_schema.dataframe_table")
    spark.sql("DROP SCHEMA IF EXISTS test_schema")
