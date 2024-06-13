import pandas as pd
import pandera as pa
from pyspark.sql import SparkSession

from dishes_forecasting.inputs.clean_data import (
    clean_recipe_price_ratings,
    clean_recipes,
    clean_weekly_dishes,
)
from dishes_forecasting.inputs.schemas import (
    RecipeIngredients,
    RecipePriceRatings,
    Recipes,
    WeeklyDishes,
)
from dishes_forecasting.logger_config import logger

spark = SparkSession.getActiveSession()


def get_input_data(
    env: str,
    company_id: str,
    input_config: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Downloading weekly dishes...")
    df_weekly_variations = get_weekly_dishes(
        env=env,
        company_id=company_id,
        min_yyyyww=input_config["min_yyyyww"],
    )

    logger.info("Downloading recipe info...")
    df_recipes = get_recipes(
        env=env,
        number_of_recipe_steps_default=input_config["number_of_recipe_steps_default"],
        cooking_time_from_default=input_config["cooking_time_from_default"],
        cooking_time_to_default=input_config["cooking_time_to_default"],
    )
    logger.info("Downloading recipe ingredients info...")
    df_recipe_ingredients = get_recipe_ingredients(env=env)

    logger.info("Downloading recipe price and ratings...")
    df_recipe_price_ratings = get_recipe_price_ratings(
        env=env,
        company_id=company_id,
        min_yyyyww=input_config["min_yyyyww"],
        average_recipe_rating_default=input_config["average_recipe_rating_default"],
        num_recipe_rating_default=input_config["num_recipe_rating_default"],
    )

    # TODO: when the data is ready
    # this should be implemented by the data model/query
    df_recipes = (
        df_recipes[
            df_recipes["recipe_portion_id"].isin(
                df_weekly_variations["recipe_portion_id"],
            )
        ]
        .drop_duplicates()
        .dropna(subset=["recipe_portion_id", "recipe_name"])
        .reset_index()
        .drop(columns="index")
    )
    df_recipe_ingredients = (
        df_recipe_ingredients[
            df_recipe_ingredients["recipe_portion_id"].isin(
                df_weekly_variations["recipe_portion_id"],
            )
        ]
        .drop_duplicates()
        .reset_index()
        .drop(columns="index")
    )

    return (
        df_weekly_variations,
        df_recipes,
        df_recipe_ingredients,
        df_recipe_price_ratings,
    )


def get_weekly_dishes(
    env: str,
    company_id: str,
    min_yyyyww: int | None = 202101,
) -> pd.DataFrame:
    df_weekly_dishes = (
        spark.read.table(f"{env}.mltesting.weekly_dish_variations")
        .select(
            "year",
            "week",
            "company_id",
            "variation_id",
            "recipe_portion_id",
            "flex_menu_recipe_order",
            "is_default",
        )
        .filter(f"company_id = '{company_id}'")
        .filter(f"year * 100 + week >= {min_yyyyww}")
        .toPandas()
    )
    df_weekly_dishes = clean_weekly_dishes(df_weekly_dishes=df_weekly_dishes)
    try:
        df_weekly_dishes = WeeklyDishes.validate(df_weekly_dishes, lazy=True)
        return df_weekly_dishes
    except pa.errors.SchemaErrors as err:
        logger.error("Schema errors and failure cases:")
        logger.error(err.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(err.data)


def get_recipe_ingredients(env: str) -> pd.DataFrame:
    df_recipe_ingredients = (
        spark.read.table(f"{env}.mltesting.recipe_ingredients")
        .select("recipe_portion_id", "ingredient_name")
        .toPandas()
    )
    try:
        df_recipe_ingredients = RecipeIngredients.validate(
            df_recipe_ingredients,
            lazy=True,
        )
        return df_recipe_ingredients
    except pa.errors.SchemaErrors as err:
        logger.error("Schema errors and failure cases:")
        logger.error(err.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(err.data)


def get_recipes(
    env: str,
    number_of_recipe_steps_default: int,
    cooking_time_from_default: int,
    cooking_time_to_default: int,
) -> pd.DataFrame:
    df_recipes = (
        spark.read.table(f"{env}.mltesting.recipe")
        .select(
            "recipe_portion_id",
            "portions",
            "recipe_name",
            "cooking_time_from",
            "cooking_time_to",
            "number_of_recipe_steps",
            "recipe_main_ingredient_name",
            "internal_tags",
            "category_tags",
            "special_food_tags",
            "recommended_tags",
        )
        .toPandas()
    )
    df_recipes = clean_recipes(
        df_recipes=df_recipes,
        number_of_recipe_steps_default=number_of_recipe_steps_default,
        cooking_time_from_default=cooking_time_from_default,
        cooking_time_to_default=cooking_time_to_default,
    )
    try:
        df_recipes = Recipes.validate(df_recipes, lazy=True)
        return df_recipes
    except pa.errors.SchemaErrors as err:
        logger.error("Schema errors and failure cases:")
        logger.error(err.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(err.data)


def get_recipe_price_ratings(
    company_id: str,
    min_yyyyww: int,
    average_recipe_rating_default: float,
    num_recipe_rating_default: int,
    env: str,
) -> pd.DataFrame:
    df_recipe_price_ratings = (
        spark.read.table(f"{env}.mltesting.recipe_price_rating")
        .select(
            "year",
            "week",
            "company_id",
            "recipe_portion_id",
            "recipe_price",
            "num_recipe_rating",
            "average_recipe_rating",
        )
        .filter(f"company_id = '{company_id}'")
        .filter(f"year * 100 + week >= {min_yyyyww}")
        .toPandas()
    )

    df_recipe_price_ratings = clean_recipe_price_ratings(
        df_recipe_price_ratings=df_recipe_price_ratings,
        average_recipe_rating_default=average_recipe_rating_default,
        num_recipe_rating_default=num_recipe_rating_default,
    )
    try:
        df_recipe_price_ratings = RecipePriceRatings.validate(
            df_recipe_price_ratings,
            lazy=True,
        )
        return df_recipe_price_ratings
    except pa.errors.SchemaErrors as err:
        logger.error("Schema errors and failure cases:")
        logger.error(err.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(err.data)
