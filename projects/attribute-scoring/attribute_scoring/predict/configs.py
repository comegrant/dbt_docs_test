import pandera.pandas as pa
import pandas as pd


class PredictionConfig:
    def __init__(self):
        pass

    def model_uri(self, env: str, company: str, target: str, alias: str) -> str:
        if not all([env, company, target]):
            raise ValueError("env, company and target must not be None")
        return f"models:/{env}.mloutputs.attribute_scoring_{company.lower()}_{target}@{alias}"

    output_columns: dict[str, type] = {
        "recipe_id": int,
        "company_id": str,
        "chefs_favorite_probability": float,
        "is_chefs_favorite": bool,
        "family_friendly_probability": float,
        "is_family_friendly": bool,
        "created_at": "datetime64[ns]",  # type: ignore
    }

    metadata_columns: list[str] = [
        "run_id",
        "run_timestamp",
        "company_id",
        "start_yyyyww",
        "end_yyyyww",
    ]

    target_mapped: dict[str, str] = {
        "has_chefs_favorite_taxonomy": "chefs_favorite",
        "has_family_friendly_taxonomy": "family_friendly",
    }
    prediction_threshold: float = 0.5
    weeks_in_future = 5

    input_schema = "gold"
    input_table = "fact_menus"
    output_schema = "mloutputs"
    output_table = "attribute_scoring"


def validate_prediction_data(df: pd.DataFrame) -> pd.DataFrame:
    schema = pa.DataFrameSchema(
        {
            "cooking_time_from": pa.Column(pa.Int32, coerce=True),
            "cooking_time_to": pa.Column(pa.Int32, coerce=True),
            "cooking_time_mean": pa.Column(pa.Float64, coerce=True),
            "recipe_difficulty_level_id": pa.Column(pa.Int32, coerce=True),
            "recipe_main_ingredient_id": pa.Column(pa.Int32, coerce=True),
            "number_of_taxonomies": pa.Column(pa.Int32, coerce=True),
            "number_of_ingredients": pa.Column(pa.Int32, coerce=True),
            "number_of_recipe_steps": pa.Column(pa.Int32, coerce=True),
            "has_chicken_filet": pa.Column(pa.Int32, coerce=True),
            "has_chicken": pa.Column(pa.Int32, coerce=True),
            "has_dry_pasta": pa.Column(pa.Int32, coerce=True),
            "has_fresh_pasta": pa.Column(pa.Int32, coerce=True),
            "has_white_fish_filet": pa.Column(pa.Int32, coerce=True),
            "has_cod_fillet": pa.Column(pa.Int32, coerce=True),
            "has_breaded_cod": pa.Column(pa.Int32, coerce=True),
            "has_salmon_filet": pa.Column(pa.Int32, coerce=True),
            "has_seafood": pa.Column(pa.Int32, coerce=True),
            "has_pork_filet": pa.Column(pa.Int32, coerce=True),
            "has_pork_cutlet": pa.Column(pa.Int32, coerce=True),
            "has_trout_filet": pa.Column(pa.Int32, coerce=True),
            "has_parmasan": pa.Column(pa.Int32, coerce=True),
            "has_cheese": pa.Column(pa.Int32, coerce=True),
            "has_minced_meat": pa.Column(pa.Int32, coerce=True),
            "has_burger_patty": pa.Column(pa.Int32, coerce=True),
            "has_noodles": pa.Column(pa.Int32, coerce=True),
            "has_sausages": pa.Column(pa.Int32, coerce=True),
            "has_tortilla": pa.Column(pa.Int32, coerce=True),
            "has_pizza_crust": pa.Column(pa.Int32, coerce=True),
            "has_bacon": pa.Column(pa.Int32, coerce=True),
            "has_wok_sauce": pa.Column(pa.Int32, coerce=True),
            "has_asian_sauces": pa.Column(pa.Int32, coerce=True),
            "has_salsa": pa.Column(pa.Int32, coerce=True),
            "has_flat_bread": pa.Column(pa.Int32, coerce=True),
            "has_pita": pa.Column(pa.Int32, coerce=True),
            "has_whole_salad": pa.Column(pa.Int32, coerce=True),
            "has_shredded_vegetables": pa.Column(pa.Int32, coerce=True),
            "has_potato": pa.Column(pa.Int32, coerce=True),
            "has_peas": pa.Column(pa.Int32, coerce=True),
            "has_rice": pa.Column(pa.Int32, coerce=True),
            "has_nuts": pa.Column(pa.Int32, coerce=True),
            "has_beans": pa.Column(pa.Int32, coerce=True),
            "has_onion": pa.Column(pa.Int32, coerce=True),
            "has_citrus": pa.Column(pa.Int32, coerce=True),
            "has_sesame": pa.Column(pa.Int32, coerce=True),
            "has_herbs": pa.Column(pa.Int32, coerce=True),
            "has_fruit": pa.Column(pa.Int32, coerce=True),
            "has_cucumber": pa.Column(pa.Int32, coerce=True),
            "has_chili": pa.Column(pa.Int32, coerce=True),
            "has_pancake": pa.Column(pa.Int32, coerce=True),
        }
    )
    return schema.validate(df)
