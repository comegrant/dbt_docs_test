import polars as pl
from preselector.model import select_next_vector


def test_next_vector_equal_importance() -> None:
    columns = ["a", "b", "c"]
    expected_vector = [0, 0, 1]
    potential_vectors = [
        [1, 0, 0, 1],
        [0, 0, 1, 2],
        [0, 1, 0, 3],
        [1, 1, 1, 4],
    ]
    target_vector = [0, 1, 1]

    new_vector = select_next_vector(
        target_vector=pl.Series(values=target_vector),
        potential_vectors=pl.DataFrame(
            data=potential_vectors,
            schema=[*columns, "basket_id"],
            orient="row",
        ),
        importance_vector=pl.Series(values=[1, 1, 1]),
        columns=columns,
        exclude_column="basket_id",
        rename_column="recipe_id",
    )

    assert new_vector.select(pl.exclude("recipe_id")).to_numpy()[0].tolist() == expected_vector


def test_next_vector() -> None:
    """
    Testing if the next vector logic is correct
    """
    columns = ["a", "b", "c"]
    expected_vector = [1, 1, 1]
    potential_vectors = [
        [1, 0, 0, 1],
        [0, 0, 1, 2],
        [0, 1, 0, 3],
        [1, 1, 1, 4],
    ]
    target_vector = [0, 1, 1]

    new_vector = select_next_vector(
        target_vector=pl.Series(values=target_vector),
        potential_vectors=pl.DataFrame(
            data=potential_vectors,
            schema=[*columns, "basket_id"],
            orient="row",
        ),
        importance_vector=pl.Series(values=[0, 1, 1]),
        columns=columns,
        exclude_column="basket_id",
        rename_column="recipe_id",
    )

    assert new_vector.select(pl.exclude("recipe_id")).to_numpy()[0].tolist() == expected_vector
