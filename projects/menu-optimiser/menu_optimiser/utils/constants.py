import os

""" Stores constants commonly used in the code """
ING_COLUMN = "main_ingredient_id"
TAX_COLUMN = "taxonomy_id"
RECIPE_ID_COLUMN = "recipe_id"
PRICE_COLUMN = "price"
PRICE_CATEGORY_COLUMN = "price_category"
COOKING_TIME_COLUMN = "cooking_time"
COOKING_TIME_FROM_COLUMN = "cooking_time_from"
COOKING_TIME_TO_COLUMN = "cooking_time_to"
RATING_COLUMN = "average_rating"
UNIVERSE_COLUMN = "is_universe"
CONSTRAINT_COLUMN = "is_constraint"
MAX_RATING = 5

RECIPE_BANK_ENV = os.getenv("RECIPE_BANK_ENV", default="QA")


class Company:
    LINAS_MATKASSE = "6A2D0B60-84D6-4830-9945-58D518D27AC2"
    ADAMS_MATKASSE = "8A613C15-35E4-471F-91CC-972F933331D7"
    GODTLEVERT = "09ECD4F0-AE58-4539-8E8F-9275B1859A19"
    RETNEMT = "5E65A955-7B1A-446C-B24F-CFE576BF52D7"


company_config = {
    "Adams_Matkasse": "AM",
    "Godtlevert": "GL",
    "Linas_Matkasse": "LMK",
    "RetNemt": "RT",
}
