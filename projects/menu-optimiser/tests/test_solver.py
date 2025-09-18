import pandas as pd
from menu_optimiser.menu_solver import (
    BusinessRule,
    solve_menu_and_storage,
    get_selected_recipes,
)
from menu_optimiser.config import OptimizationConfig
import pytest


data = pd.DataFrame(
    {
        "recipe_id": [1, 2, 3],
        "taxonomies": [[20], [20], [20]],
        "main_protein": [["Chicken"], ["Chicken"], ["Chicken"]],
        "main_carb": [["Rice"], ["Rice"], ["Potato"]],
        "cuisine": [[2], [1], [1]],
        "dish_type": [[1], [3], [1]],
        "protein_processing": ["filet"] * 3,
        "cooking_time_to": [40, 45, 30],
        "price_category_id": [0, 1, 0],
        "main_ingredient_id": [1] * 3,
        "average_rating": [4.5] * 3,
    }
)

required_data = pd.DataFrame(data[data["recipe_id"] == 4])  # create empty df

ingredients = pd.DataFrame(
    {
        "recipe_id": [1, 1, 1, 2, 2, 3, 3],
        "ingredient_id": [4, 5, 6, 7, 8, 9, 10],
        "ingredient_name": ["ch", "ri", "ex", "po", "to", "on", "ga"],
        "is_cold_storage": [True, False, True, True, True, False, False],
    }
)


@pytest.mark.asyncio
async def test_linked_recipes_storage():
    df = data.copy()
    required_df = required_data.copy()
    ingredients_df = ingredients.copy()

    taxonomy_id = 20
    taxonomy_requests = {
        taxonomy_id: BusinessRule(
            total=2,
            main_ingredients={1: 2},
            price_category={0: 0},
            cooking_time_to={30: 0},
        ),
    }

    company_config = OptimizationConfig(
        max_cold_storage=3,
        max_dry_storage=3,
        diversity_config={
            "main_protein": 2.0,
        },
    )

    total_req_tax_recipes = 2

    linked_recipes = {
        "1": [2],
        "2": [3],
    }

    solver, _ = await solve_menu_and_storage(
        available_recipes_df=df,
        required_recipes_df=required_df,
        ingredients_df=ingredients_df,
        taxonomy_constraints=taxonomy_requests,
        total_required_recipes=total_req_tax_recipes,
        company_config=company_config,
        linked_recipes=linked_recipes,
    )

    assert solver.status == 1
    assert set(get_selected_recipes(solver)) == {1, 3}


@pytest.mark.asyncio
async def test_diveristy_constraints():
    """Test the diversity constraints."""

    df = data.copy()
    ingredients_df = ingredients.copy()
    required_df = required_data.copy()

    taxonomy_id = 20
    taxonomy_requests = {
        taxonomy_id: BusinessRule(
            total=2,
            main_ingredients={1: 2},
            price_category={0: 0},
            cooking_time_to={30: 0},
        ),
    }

    diversity_mapping_1 = {"main_carb": 2.0, "dish_type": 2.0}  # should give 2 and 3
    diversity_mapping_2 = {"main_carb": 2.0, "cuisine": 2.0}  # should give 1 and 3

    company_config1 = OptimizationConfig(
        max_cold_storage=3,
        max_dry_storage=3,
        diversity_config=diversity_mapping_1,
    )

    company_config2 = OptimizationConfig(
        max_cold_storage=3,
        max_dry_storage=3,
        diversity_config=diversity_mapping_2,
    )

    total_req_tax_recipes = 2

    solver_1, _ = await solve_menu_and_storage(
        available_recipes_df=df,
        required_recipes_df=required_df,
        ingredients_df=ingredients_df,
        taxonomy_constraints=taxonomy_requests,
        total_required_recipes=total_req_tax_recipes,
        company_config=company_config1,
        linked_recipes=None,
    )

    solver_2, _ = await solve_menu_and_storage(
        available_recipes_df=df,
        required_recipes_df=required_df,
        ingredients_df=ingredients_df,
        taxonomy_constraints=taxonomy_requests,
        total_required_recipes=total_req_tax_recipes,
        company_config=company_config2,
        linked_recipes=None,
    )

    assert solver_1.status == 1 and solver_2.status == 1
    assert set(get_selected_recipes(solver_1)) == {2, 3}
    assert set(get_selected_recipes(solver_2)) == {1, 3}


@pytest.mark.asyncio
async def test_business_rules():
    """Test the business rules."""

    df = data.copy()
    ingredients_df = ingredients.copy()
    required_df = required_data.copy()

    taxonomy_requests = {
        20: BusinessRule(
            total=1,
            main_ingredients={1: 1},
            price_category={0: 1},
            cooking_time_to={30: 1},
        ),
    }

    company_config = OptimizationConfig(
        max_cold_storage=3,
        max_dry_storage=3,
        diversity_config={"protein_processing": 1.0},
    )

    total_req_tax_recipes = 1

    solver, _ = await solve_menu_and_storage(
        available_recipes_df=df,
        required_recipes_df=required_df,
        ingredients_df=ingredients_df,
        taxonomy_constraints=taxonomy_requests,
        total_required_recipes=total_req_tax_recipes,
        company_config=company_config,
    )

    assert solver.status == 1
    assert set(get_selected_recipes(solver)) == {3}


@pytest.mark.asyncio
async def test_ingredient_storage():
    """Test the max ingredient storage."""

    df = data.copy()
    ingredients_df = ingredients.copy()
    required_df = required_data.copy()

    taxonomy_requests = {
        20: BusinessRule(
            total=1,
            main_ingredients={1: 1},
            price_category={0: 1},
            cooking_time_to={30: 1},
        ),
    }

    company_config_1 = OptimizationConfig(
        max_cold_storage=2,
        max_dry_storage=1,
        diversity_config={"main_carb": 2.0, "cuisine": 2.0},
    )

    company_config_2 = OptimizationConfig(
        max_cold_storage=3,
        max_dry_storage=1,
        diversity_config={"main_carb": 2.0, "cuisine": 2.0},
    )

    total_req_tax_recipes = 2

    solver_1, error_1 = await solve_menu_and_storage(
        available_recipes_df=df,
        required_recipes_df=required_df,
        ingredients_df=ingredients_df,
        taxonomy_constraints=taxonomy_requests,
        total_required_recipes=total_req_tax_recipes,
        company_config=company_config_1,
    )

    solver_2, error_2 = await solve_menu_and_storage(
        available_recipes_df=df,
        required_recipes_df=required_df,
        ingredients_df=ingredients_df,
        taxonomy_constraints=taxonomy_requests,
        total_required_recipes=total_req_tax_recipes,
        company_config=company_config_2,
    )
    assert solver_1.status == 1
    assert set(get_selected_recipes(solver_1)) == {3}
    assert error_1 == "Removed 1 recipe(s) to fit storage constraints."

    assert solver_2.status == 1
    assert set(get_selected_recipes(solver_2)) == {2, 3}
    assert error_2 == "Adjusted storage: cold: 2, dry: 2"


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_linked_recipes_storage())
