import polars as pl
from preselector.main import select_next_vector


def test_next_vector() -> None:
    columns = ["a", "b", "c"]
    expected_vector = [0, 0, 1]
    available_vectors = [
        [1, 0, 0, 1],
        [0, 0, 1, 2],
        [0, 1, 0, 3],
        [1, 1, 1, 4],
    ]
    target_vector = [0, 1, 1]
    current_vector = [0, 1, 0]

    new_vector = select_next_vector(
        pl.Series(values=current_vector),
        pl.Series(values=target_vector),
        pl.DataFrame(
            data=available_vectors,
            schema=[*columns, "basket_id"],
            orient="row",
        ),
        normalization_vector=pl.Series(values=[1, 1, 1]),
        columns=columns,
        exclude_column="basket_id",
        rename_column="recipe_id",
    )

    assert new_vector.select(pl.exclude("recipe_id")).to_numpy()[0].tolist() == expected_vector
