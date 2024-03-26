import pandas as pd


def evaluate_predictions(
    predictions: pd.DataFrame,
    recipe_taxonomies: pd.DataFrame,
    on_key: str = "recipe_id",
) -> None:
    joined = predictions.merge(recipe_taxonomies, how="inner", on=on_key)
    crate_taxonomies = (
        joined.groupby("agreement_id")["recipe_taxonomies"]
        .apply(lambda group: ",".join(group))
        .str.split(",")
        .reset_index()
    )

    return crate_taxonomies.assign(
        unique_taxonomie_count=crate_taxonomies["recipe_taxonomies"].apply(
            lambda row: len(set(row)),
        ),
    )
