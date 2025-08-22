import pandas as pd
from attribute_scoring.train.preprocessing import generate_sample
from unittest.mock import Mock, patch
from attribute_scoring.predict.data import extract_features
from sklearn.pipeline import Pipeline
from attribute_scoring.train.model import model
from attribute_scoring.common import ArgsTrain


def test_generate_sample() -> None:
    df = pd.DataFrame({"feature": range(100), "target": [True, False] * 50})

    sample = generate_sample(df, "target", sample_size=20)
    assert len(sample) == 20

    target_dist = sample["target"].value_counts(normalize=True)
    assert abs(target_dist[True] - 0.5) < 0.2


@patch("attribute_scoring.predict.data.DATA_CONFIG")
def test_extract_features(mock_config) -> None:
    mock_config.recipe_features.feature_names = ["feature_1", "feature_2"]
    mock_config.ingredient_features.feature_names = ["feature_3"]

    test_data = pd.DataFrame(
        {
            "feature_1": [1, 2, 3],
            "feature_2": [4, 5, 6],
            "not_feature": ["a", "b", "c"],
            "feature_3": [7, 8, 9],
        }
    )

    features = extract_features(test_data)

    assert len(features.columns) == 3
    assert "feature_1" in features.columns
    assert "feature_2" in features.columns
    assert "feature_3" in features.columns
    assert "not_feature" not in features.columns


def test_model_pipeline_structure() -> None:
    args = ArgsTrain(
        company="AMK", target="has_chefs_favorite_taxonomy", env="dev", alias="champion"
    )
    mock_classifier = Mock()

    pipeline = model(args, classifier=mock_classifier)

    assert isinstance(pipeline, Pipeline)
    assert len(pipeline.steps) == 2
    assert pipeline.steps[0][0] == "preprocessor"
    assert pipeline.steps[1][0] == "classifier"
