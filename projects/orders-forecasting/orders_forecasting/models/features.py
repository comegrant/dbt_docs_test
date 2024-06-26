from constants.companies import Company
from orders_forecasting.paths import CONFIG_DIR
from orders_forecasting.utils import read_yaml
from pydantic import BaseModel


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


def get_features_config(company: Company) -> FeaturesConfig:
    feature_sets = get_feature_sets_by_company(company)
    feature_tables = []
    features_config = read_yaml(file_name="features", directory=CONFIG_DIR)
    for feature_set in feature_sets:
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
        target_container=features_config["target_container"],
        source_catalog_schema=features_config["source_catalog_schema"],
        source_catalog_name=features_config["source_catalog_name"],
    )


def get_feature_sets_by_company(company: Company) -> list:
    return [
        "order_history_features",
        "order_estimation_features",
        "order_estimation_history_retention_features",
        f"holidays_features_{company.country.lower()}",
    ]
