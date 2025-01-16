from pydantic import BaseModel


class FeatureLookUpConfig(BaseModel):
    features_container: str = "mlfeatures"
    feature_table_name: str
    primary_keys: list[str] = []
    feature_columns: list[str] = []
    exclude_in_training_set: list[str] = []


weekly_dishes_variations_lookup = FeatureLookUpConfig(
    feature_table_name="ft_weekly_dishes_variations",
    primary_keys=[
        "menu_year",
        "menu_week",
        "company_id",
        "product_variation_id",
    ],
    feature_columns=[
        "language_id",
        "recipe_id",
        "recipe_portion_id",
        "portion_quantity",
        "product_variation_name"
    ],
    exclude_in_training_set=[
        "product_variation_id",
        "company_id",
        "language_id",
        "recipe_id",
        "recipe_portion_id",
        "product_variation_name"
    ]
)

recipes_lookup = FeatureLookUpConfig(
    feature_table_name="ft_ml_recipes",
    primary_keys=[
        "recipe_id",
    ],
    feature_columns=[
        "recipe_name",
        "cooking_time_from",
        "cooking_time_to",
        "recipe_difficulty_level_id",
        "recipe_main_ingredient_id",
        "taxonomy_list",
        "number_of_taxonomies",
        "has_chefs_favorite_taxonomy",
        "has_family_friendly_taxonomy",
        "has_quick_and_easy_taxonomy",
        "has_vegetarian_taxonomy",
        "has_low_calorie_taxonomy",
        "number_of_recipe_steps",
        "number_of_ingredients"
    ],
    exclude_in_training_set=[
        "recipe_id",
    ]
)


recipe_ingredients_lookup = FeatureLookUpConfig(
    feature_table_name="ft_recipe_ingredients",
    primary_keys=[
        "recipe_portion_id",
        "language_id"
    ],
    feature_columns=[
        "has_chicken_filet",
        "has_chicken",
        "has_dry_pasta",
        "has_white_fish_filet",
        "has_cod_fillet",
        "has_breaded_cod",
        "has_salmon_filet",
        "has_seafood",
        "has_pork_filet",
        "has_pork_cutlet",
        "has_trout_filet",
        "has_parmasan",
        "has_cheese",
        "has_minced_meat",
        "has_burger_patty",
        "has_noodles",
        "has_sausages",
        "has_pizza_crust",
        "has_bacon",
        "has_wok_sauce",
        "has_asian_sauces",
        "has_salsa",
        "has_flat_bread",
        "has_pita",
        "has_potato",
        "has_rice",
        "has_nuts",
        "has_chili",
    ],
    exclude_in_training_set=[
        "recipe_portion_id",
        "language_id",
    ]
)

feature_lookup_config_list = [
    weekly_dishes_variations_lookup,
    recipes_lookup,
    recipe_ingredients_lookup
]
