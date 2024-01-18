import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def rank_recipes(
    recipes: pd.DataFrame, cluster_column: str = "cluster", score_column: str = "score"
) -> pd.Series:
    """
    Ranks a set of recipes based on the score and cluster they belong to.

    NB! This method assumes that the data frame is only for one user / agreement.

    ```python
    input_df = pd.DataFrame({
        "cluster": [1, 2, 1, 2, 1, 2, 3],
        "score":   [1, 3, 2, 2, 4, 9, 4],
    })
    result = rank_recipes(input_df).sort_index()
    print(result.tolist())
    >>> [0, 4, 3, 1, 5, 6, 2]
    ```
    """
    if score_column not in recipes.columns:
        raise ValueError(f"Needs a {score_column} column to rank recipes")
    if cluster_column not in recipes.columns:
        raise ValueError(f"Needs a {cluster_column} column to rank recipes")

    for value in recipes[cluster_column].unique():
        mask = recipes[cluster_column] == value
        recipes.loc[mask, "cluster_rank"] = recipes.loc[mask, score_column].rank(
            method="min", ascending=False
        )

    sorted_recipes = recipes.sort_values(
        by=["cluster_rank", score_column], ascending=[True, False]
    )
    sorted_recipes["order_of_relevance_cluster"] = range(1, sorted_recipes.shape[0] + 1)

    return sorted_recipes["order_of_relevance_cluster"]


def predict_order_of_relevance(recipes: pd.DataFrame) -> pd.DataFrame:
    # Need to do the if, as the `level_1` will not exist if containing only one ID
    if len(recipes["agreement_id"].unique()) > 1:
        subset_ranking = (
            recipes.groupby("agreement_id")
            .apply(rank_recipes)
            .reset_index()
            .set_index("level_1")
        )
        return recipes.join(subset_ranking["order_of_relevance_cluster"])
    else:
        recipes["order_of_relevance_cluster"] = rank_recipes(recipes)
        return recipes


def predict_rankings(
    recipes: pd.DataFrame, menus: pd.DataFrame, id_column: str = "recipe_id"
) -> pd.DataFrame:
    rankings: pd.DataFrame | None = None

    for year_week in menus["yearweek"].unique():
        logger.info(f"Predicting recommendation ratings for year week {year_week}")
        mask = menus["yearweek"] == year_week

        menu_week = menus[mask]
        recipe_ids = menu_week[id_column]

        features_mask = recipes[id_column].isin(recipe_ids)
        sub_features = recipes[features_mask]

        preds = predict_order_of_relevance(sub_features.copy())
        with_product_id = preds.merge(
            menu_week[[id_column, "product_id"]], how="inner", on=id_column
        )
        week = year_week % 100
        with_product_id["week"] = week
        with_product_id["year"] = int((year_week - week) / 100)

        if rankings is not None:
            rankings = pd.concat([with_product_id, rankings], axis=0)
        else:
            rankings = with_product_id

    if rankings is None:
        raise ValueError("No ranking predictions were produced.")

    rankings["predicted_at"] = datetime.utcnow()

    logger.info(f"Produced rankings {rankings.head()}")
    rankings.to_csv("data/rankings_debug.csv")
    return rankings
