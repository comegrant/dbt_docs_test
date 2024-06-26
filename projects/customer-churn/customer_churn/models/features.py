from pydantic import BaseModel

from customer_churn.paths import CONFIG_DIR
from customer_churn.utils import read_yaml


class ColumnsConfig(BaseModel):
    primary_keys: list
    feature_columns: list
    timeseries_columns: list = []


class FeatureTableConfig(BaseModel):
    name: str
    columns: ColumnsConfig
    description: str = ""


class FeaturesConfig(BaseModel):
    feature_tables: list[FeatureTableConfig]
    feature_container_name: str
    target_table: FeatureTableConfig
    target_container: str
    source_catalog_schema: str
    source_catalog_name: str
    target_col: str


def get_features_config() -> FeaturesConfig:
    feature_tables = []
    features_config = read_yaml(file_name="features", directory=CONFIG_DIR)
    for feature_set in features_config["feature_tables"]:
        feature_set_config = features_config["feature_tables"][feature_set]
        feature_tables.append(
            FeatureTableConfig(
                name=feature_set,
                columns=ColumnsConfig(
                    primary_keys=feature_set_config["columns"]["primary_keys"],
                    feature_columns=feature_set_config["columns"]["feature_columns"],
                    timeseries_columns=feature_set_config["columns"].get(
                        "timeseries_columns", []
                    ),
                ),
            )
        )

    return FeaturesConfig(
        feature_tables=feature_tables,
        feature_container_name=features_config["feature_container_name"],
        target_table=FeatureTableConfig(
            name=features_config["target_table"]["name"],
            columns=ColumnsConfig(
                feature_columns=features_config["target_table"]["columns"][
                    "feature_columns"
                ],
                primary_keys=features_config["target_table"]["columns"]["primary_keys"],
                timeseries_columns=features_config["target_table"]["columns"].get(
                    "timeseries_columns", []
                ),
            ),
        ),
        target_col=features_config["target_col"],
        target_container=features_config["target_container"],
        source_catalog_schema=features_config["source_catalog_schema"],
        source_catalog_name=features_config["source_catalog_name"],
    )
