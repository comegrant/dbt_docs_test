import pandera as pa
from pandera.typing import Series


class WeeklyDishes(pa.DataFrameModel):
    year: Series[int] = pa.Field(in_range={"min_value": 2000, "max_value": 3000})
    week: Series[int] = pa.Field(in_range={"min_value": 1, "max_value": 53})
    company_id: Series[str] = pa.Field(str_length=36)
    variation_id: Series[str] = pa.Field(str_length=36)
    recipe_portion_id: Series[int]
    flex_menu_recipe_order: Series[int]
    is_default: Series[int]


class Recipes(pa.DataFrameModel):
    recipe_portion_id: Series[int]
    portions: Series[int]
    cooking_time_from: Series[int]
    cooking_time_to: Series[int]
    number_of_recipe_steps: Series[int]
    recipe_name: Series[str] = pa.Field(nullable=True)
    recipe_main_ingredient_name: Series[str]
    internal_tags: Series[str] = pa.Field(nullable=True)
    category_tags: Series[str] = pa.Field(nullable=True)
    special_food_tags: Series[str] = pa.Field(nullable=True)
    recommended_tags: Series[str] = pa.Field(nullable=True)


class RecipeIngredients(pa.DataFrameModel):
    recipe_portion_id: Series[int]
    ingredient_name: Series[str] = pa.Field(nullable=True)


class RecipePriceRatings(pa.DataFrameModel):
    year: Series[int] = pa.Field(in_range={"min_value": 2000, "max_value": 3000})
    week: Series[int] = pa.Field(in_range={"min_value": 1, "max_value": 53})
    company_id: Series[str] = pa.Field(str_length=36)
    recipe_portion_id: Series[int]
    recipe_price: Series[float]
    num_recipe_rating: Series[int]
    average_recipe_rating: Series[float] = pa.Field(
        in_range={"min_value": 0, "max_value": 5.0},
    )
