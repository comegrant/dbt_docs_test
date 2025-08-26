import pandas as pd
from attribute_scoring.train.preprocessing import generate_sample
from unittest.mock import Mock
from sklearn.pipeline import Pipeline
from attribute_scoring.train.model import model
from attribute_scoring.common import ArgsTrain


def test_generate_sample() -> None:
    df = pd.DataFrame({"feature": range(100), "target": [True, False] * 50})

    sample = generate_sample(df, "target", sample_size=20)
    assert len(sample) == 20

    target_dist = sample["target"].value_counts(normalize=True)
    assert abs(target_dist[True] - 0.5) < 0.2


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
