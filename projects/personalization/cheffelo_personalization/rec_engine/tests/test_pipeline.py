import json

import pandas as pd
import pytest
import pytest_asyncio
from aligned import FeatureStore, FileSource
from aligned.compiler.model import uuid4
from aligned.feature_source import BatchFeatureSource
from aligned.schemas.feature import FeatureLocation

from cheffelo_personalization.blob_storage import LocalFolder
from cheffelo_personalization.rec_engine.data.store import recommendation_contracts
from cheffelo_personalization.rec_engine.run import (
    ManualDataset,
    RateMenuRecipes,
    format_ranking_recommendations,
    run,
)


@pytest_asyncio.fixture
async def model_contracts() -> FeatureStore:
    store = recommendation_contracts()

    test_folder = LocalFolder("test_data")

    location_source_map = {
        FeatureLocation.feature_view("recipe_taxonomies"): test_folder.csv_at(
            "recipe.csv"
        ),
        FeatureLocation.feature_view("historical_recipe_orders"): test_folder.csv_at(
            "recipe_rating.csv"
        ),
        FeatureLocation.feature_view("recipe_ingredients"): test_folder.csv_at(
            "recipe_ingredients.csv"
        ),
    }

    return store.with_source(
        BatchFeatureSource(
            {loc.identifier: source for loc, source in location_source_map.items()}
        )
    )


@pytest.mark.asyncio
async def test_training_pipelines(model_contracts: FeatureStore):
    nr_agreements = 2
    dataset = ManualDataset(
        train_on_recipe_ids=[1, 2, 3, 4],
        rate_menus=RateMenuRecipes(
            year_weeks=[
                202344,
                202344,
                202345,
                202345,
                202344,
                202345,
                202344,
                202345,
                202344,
                202345,
            ],
            recipe_ids=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            product_id=["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        ),
    )
    nr_expected_recommendations = len(dataset.rate_menus.recipe_ids) * nr_agreements

    test_run = str(uuid4())
    write_path = f"data/rec_engine/{test_run}"

    await run(
        dataset=dataset,
        store=model_contracts,
        write_to_path=write_path,
        update_source_threshold=None,  # Setting None makes sure we do not update
        ratings_update_source_threshold=None,
    )

    rec_engine_file = FileSource.csv_at(f"{write_path}/rec_engine.csv")
    recs = await rec_engine_file.to_pandas()
    assert recs.shape[0] == nr_expected_recommendations


def test_json_formatting():
    """
    Tests that we are able to format a list of product ids with a order of relevence into the json format that frontend expects.

    Expected output associated with an agreement id:
    ```json
    [
        {"year": 2023,"week": 45,"productIds":["5934A129-8FE1-4DCD-9402-E1AD8E820F29","F45361C7-D809-4C10-8A3E-5B92F6F3EEAB", ...]},
        {"year": 2023,"week": 47,"productIds":["5934A129-8FE1-4DCD-9402-E1AD8E820F29","F45361C7-D809-4C10-8A3E-5B92F6F3EEAB", ...]},
        {"year": 2023,"week": 46,"productIds":["5934A129-8FE1-4DCD-9402-E1AD8E820F29","F45361C7-D809-4C10-8A3E-5B92F6F3EEAB", ...]},
        {"year": 2023,"week": 49,"productIds":["B9902756-2171-4891-AF41-7C103764FC09","841E252E-7F0C-4190-B091-6AA15C53F68B", ...]},
        {"year": 2023,"week": 48,"productIds":["841E252E-7F0C-4190-B091-6AA15C53F68B","EE7267ED-D4F3-4936-AFA3-E2C8426B45C3", ...]}
    ]
    ```
    """

    input_df = pd.DataFrame(
        {
            "product_id": ["1", "2", "3", "4", "1", "2", "3", "4"] * 2,
            "order_of_relevance_cluster": [1, 2, 3, 4, 4, 3, 2, 1] * 2,
            "year": [2023] * 8 * 2,
            "week": [44, 44, 44, 44, 43, 43, 43, 43] * 2,
            "agreement_id": [1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2],
        }
    )

    expected_list = [
        {"year": 2023, "week": 44, "productIds": ["1", "2", "3"]},
        {"year": 2023, "week": 43, "productIds": ["4", "3", "2"]},
    ]
    json_response = json.dumps(expected_list)
    expected_df = pd.DataFrame(
        {
            "agreement_id": [1, 2],
            "recommendation_json": [json_response, json_response],
        }
    )

    output_df = format_ranking_recommendations(input_df, 3)

    for _, row in expected_df.iterrows():
        output_row = output_df[output_df["agreement_id"] == row["agreement_id"]].iloc[0]
        output_json = output_row["recommendation_json"]
        output_list = json.loads(output_json)

        assert len(output_list) == len(expected_list)

        for expected_item in expected_list:
            output_item: dict = [
                i
                for i in output_list
                if i["year"] == expected_item["year"]
                and i["week"] == expected_item["week"]
            ][0]

            for key in expected_item.keys():
                assert expected_item[key] == output_item[key]
