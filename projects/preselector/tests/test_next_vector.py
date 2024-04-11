import polars as pl
from preselector.new_main import run_preselector, select_next_vector


def test_next_vector():
    columns = ["a", "b", "c"]
    expected_vector = [0, 0, 1]
    available_vectors = [
        [1, 0, 0],
        [0, 0, 1],
        [0, 1, 0],
        [1, 1, 1],
    ]
    target_vector = [0, 1, 1]
    current_vector = [0, 1, 0]

    new_vector = select_next_vector(
        pl.Series(values=current_vector),
        pl.Series(values=target_vector),
        pl.DataFrame(data=available_vectors, schema=columns, orient="row"),
    )

    assert new_vector.to_numpy()[0].tolist() == expected_vector


def test_optimal_combination():
    columns = ["a", "b", "c", "recipe_id"]
    expected_recipe_ids = [1, 4]
    available_vectors = [
        [1, 0, 0, 1],
        [0, 0, 1, 2],
        [0, 1, 0, 3],
        [1, 1, 1, 4],
    ]
    target_vector = [2, 1, 1]

    target = pl.Series(values=target_vector)
    recipes = pl.DataFrame(data=available_vectors, schema=columns, orient="row")

    combination = run_preselector(target, recipes, 2)

    assert combination.height == 2
    assert combination["recipe_id"].sort(descending=False).to_list() == expected_recipe_ids
