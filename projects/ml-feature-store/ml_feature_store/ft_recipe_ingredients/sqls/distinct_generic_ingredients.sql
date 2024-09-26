select distinct
    language_id,
    generic_ingredient_id,
    generic_ingredient_name
from {env}.{schema}.recipe_ingredients
