import numpy as np
import pandas as pd
import pytest
from ml_example_project.train.configs import get_company_train_configs
from ml_example_project.train.model import define_model_pipeline
from ml_example_project.train.preprocessor import PreProcessor


@pytest.mark.asyncio
async def test_preprocessor() -> None:
    data = pd.DataFrame(
        {
            "number_of_ingredients": [5, 10, 15],
            "cooking_time_from": [1, 2, 1],
        }
    )

    preprocessor = PreProcessor(numeric_features=["number_of_ingredients"], categorical_features=["cooking_time_from"])

    scaled_data = preprocessor.fit_transform(data)

    assert scaled_data.shape == (3, 3)
    assert np.isclose(scaled_data[:, 0].mean(), 0)
    assert np.isclose(scaled_data[:, 0].std(), 1)
    assert np.array_equal(scaled_data[:, 1], np.array([1, 0, 1]))


@pytest.mark.asyncio
async def test_pipeline() -> None:
    data = pd.DataFrame(
        {
            "number_of_ingredients": [5, 10, 15],
            "number_of_taxonomies": [1, 2, 3],
            "cooking_time_from": [1, 2, 1],
            "recipe_difficulty_level_id": [1, 1, 1],
        }
    )

    X = pd.DataFrame(data[["number_of_ingredients", "number_of_taxonomies", "cooking_time_from"]])  # noqa
    y = pd.Series(data["recipe_difficulty_level_id"])

    company_train_configs = get_company_train_configs(company_code="LMK")

    pipeline = define_model_pipeline(model_params=company_train_configs.model_params, task="classify")
    pipeline.fit(X, y)
    prediction = pipeline.predict(model_input=X, context=None)

    assert prediction.shape == (3,)
    assert np.array_equal(prediction, np.array([1, 1, 1]))


@pytest.mark.asyncio
async def test_prediction() -> None:
    data = pd.DataFrame(
        {
            "number_of_ingredients": [3, 7, 10, 12, 15],
            "number_of_taxonomies": [1, 2, 3, 4, 5],
            "cooking_time_from": [10, 30, 50, 60, 90],
            "recipe_difficulty_level_id": [1, 2, 2, 3, 3],
        }
    )

    X = pd.DataFrame(data[["number_of_ingredients", "number_of_taxonomies", "cooking_time_from"]])  # noqa
    y = pd.Series(data["recipe_difficulty_level_id"])

    company_train_configs = get_company_train_configs(company_code="LMK")

    pipeline = define_model_pipeline(model_params=company_train_configs.model_params, task="classify")

    pipeline.fit(X, y)

    n_sim = 50
    predictions = np.array([pipeline.predict(model_input=X, context=None) for _ in range(n_sim)])

    avg_predictions = np.mean(np.array(predictions), axis=0)

    assert avg_predictions.shape == (5,)
    assert avg_predictions[0] < avg_predictions[-1]
    assert not np.array_equal(avg_predictions, np.array([3, 2, 1, 1, 3]))

    np.testing.assert_array_almost_equal(avg_predictions, np.array([1, 2, 2, 3, 3]), decimal=1)
