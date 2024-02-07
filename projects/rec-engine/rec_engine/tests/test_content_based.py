import unittest
from unittest import TestCase

import pandas as pd
from rec_engine.content_based import CBModel
from rec_engine.ranking_model import (
    predict_order_of_relevance,
    rank_recipes,
)


class TestPredict(TestCase):
    """This class holds all the tests related to most of the methods inside
    the predict.py file and Predict class."""

    def test_content_based(self) -> None:
        ratings = pd.DataFrame(
            {
                "agreement_id": [1, 2, 2, 1, 2],
                "recipe_id": [1, 1, 2, 2, 3],
                "rating": [3, 3, 1, 3, 5],
            },
        )
        model = CBModel.train(
            recipes=pd.DataFrame(
                {
                    "recipe_taxonomies": ["rask,laktosefri", "rask", "laktosefri"],
                    "all_ingredients": ["tomat, laks", "spagetti,kjøtt", "kylling"],
                    "recipe_id": [1, 2, 3],
                },
            ),
            ratings=ratings,
            model_contract_name="test",
        )
        number_of_customers = ratings["agreement_id"].unique().shape[0]

        recipe_ids_to_predict = [10, 13]

        recipe_to_predict = pd.DataFrame(
            data={
                "recipe_taxonomies": ["rask", "laktosefri"],
                "all_ingredients": ["kylling", "laks, kylling"],
            },
            index=recipe_ids_to_predict,
        )
        preds = model.predict(recipe_to_predict)

        assert preds.shape[0] == recipe_to_predict.shape[0] * number_of_customers
        for column in ["agreement_id", "recipe_id", "score"]:
            assert column in preds.columns

        assert preds["recipe_id"].isin(recipe_ids_to_predict).all()

    def test_ranking(self) -> None:
        expected = pd.Series([7, 4, 5, 6, 2, 1, 3])
        input_df = pd.DataFrame(
            {
                "cluster": [1, 2, 1, 2, 1, 2, 3],
                "score": [1, 3, 2, 2, 5, 9, 4],
            },
        )
        result = rank_recipes(input_df)
        ordered_rank = result.sort_index()
        assert expected.equals(ordered_rank)

    def test_predict_for_recipe_scorings(self) -> None:
        expected = pd.DataFrame(
            {
                "agreement_id": [1, 1, 1, 2, 2, 2, 3],
                "recipe_id": [1, 2, 3, 1, 2, 3, 1],
                "order_of_relevance_cluster": [2, 3, 1, 3, 1, 2, 1],
            },
        )
        recipes = pd.DataFrame(
            {
                "score": [10, 1, 17, 20, 1, 17, 10, 2, 17, 5],
                "cluster": [1, 1, 2, 2, 1, 1, 2, 2, 1, 2],
                "recipe_id": [1, 2, 3, 4, 1, 2, 3, 4, 1, 4],
                "agreement_id": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3],
            },
        )
        recipe_subset = recipes[recipes["recipe_id"].isin([1, 2, 3])]
        result = predict_order_of_relevance(recipe_subset)
        assert result.shape[0] == expected.shape[0]
        assert expected.equals(
            result[
                ["agreement_id", "recipe_id", "order_of_relevance_cluster"]
            ].reset_index(drop=True),
        )


if __name__ == "__main__":
    unittest.main()
