from typing import Literal

from constants.companies import get_company_by_code
from databricks.feature_engineering import FeatureEngineeringClient
from pydantic import BaseModel

from dishes_forecasting.features.build_features import build_features
from dishes_forecasting.features.save_features import save_feature_table
from dishes_forecasting.inputs.get_data import get_input_data
from dishes_forecasting.logger_config import logger
from dishes_forecasting.paths import CONFIG_DIR
from dishes_forecasting.utils import read_yaml


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]


def run_features(args: Args) -> None:
    company_code = args.company
    logger.info(f"Running feature generation step for {company_code}...")
    company = get_company_by_code(company_code=company_code)
    company_id = company.company_id
    logger.info("Fetching configs for ...")
    input_configs = read_yaml(directory=CONFIG_DIR, filename="inputs.yml")

    input_config = input_configs[company_code]

    feature_configs = read_yaml(directory=CONFIG_DIR, filename="features.yml")

    logger.info("Fetching data...")
    (
        # df_flex_orders,
        df_weekly_variations,
        df_recipes,
        df_recipe_ingredients,
        df_recipe_price_ratings,
    ) = get_input_data(company_id=company_id, input_config=input_config, env=args.env)

    logger.info("Creating feature dataframes...")
    df_weekly_dishes_features, df_recipe_features = build_features(
        df_weekly_variations=df_weekly_variations,
        df_recipes=df_recipes,
        df_recipe_ingredients=df_recipe_ingredients,
        df_recipe_price_ratings=df_recipe_price_ratings,
        feature_configs=feature_configs,
        language=company.language,
    )
    logger.info("Saving feature tables...")
    fe = FeatureEngineeringClient()
    feature_container_name = feature_configs["feature_container_name"]
    weekly_dishes_features_configs = feature_configs["feature_tables"][
        "weekly_dishes_features"
    ]
    recipe_features_configs = feature_configs["feature_tables"]["recipe_features"]

    logger.info("Saving weekly_dishes_features...")
    save_feature_table(
        fe=fe,
        df=df_weekly_dishes_features,
        env=args.env,
        feature_container=feature_container_name,
        feature_table_name=weekly_dishes_features_configs["name"],
        columns=weekly_dishes_features_configs["columns"],
    )

    logger.info("Saving recipe features...")
    save_feature_table(
        fe=fe,
        df=df_recipe_features,
        env=args.env,
        feature_container=feature_container_name,
        feature_table_name=recipe_features_configs["name"],
        columns=recipe_features_configs["columns"],
    )
