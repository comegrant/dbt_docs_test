import logging
from typing import Literal

from constants.companies import get_company_by_code
from databricks.feature_engineering import FeatureEngineeringClient
from pydantic import BaseModel

from orders_forecasting.features.build_features import get_features
from orders_forecasting.features.save_features import save_feature_table
from orders_forecasting.models.company import get_company_config_for_company
from orders_forecasting.models.features import get_features_config

logger = logging.getLogger(__name__)


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]


def generate_features(args: Args) -> None:
    company = get_company_by_code(args.company)
    company_config = get_company_config_for_company(company=company)
    features_config = get_features_config(company=company)

    df_features, df_order_history, df_estimations, df_holiday_features = get_features(
        company=company,
        company_config=company_config,
        features_config=features_config,
    )

    feature_data = {
        "order_estimation_history_retention_features": df_features,
        "order_history_features": df_order_history,
        "order_estimation_features": df_estimations,
        f"holidays_features_{company.country.lower()}": df_holiday_features,
    }

    df_features.sort_values(by=["year", "week"], inplace=True)

    fe = FeatureEngineeringClient()
    for table in features_config.feature_tables:
        save_feature_table(
            fe=fe,
            df=feature_data[table.name],
            env=args.env,
            feature_container=features_config.feature_container_name,
            feature_table_name=table.name,
            columns=table.columns,
            table_description=table.description,
        )
