import pandas as pd
import polars as pl
import pytest
from embeddings.sentence_transformer import SentenceTransformer

input_column = "text"
input_data = ["Hello world", "Welcome to the data kicthen"]


@pytest.mark.asyncio
async def test_sentence_transformer_list() -> None:
    model = SentenceTransformer("sentence-transformers/paraphrase-albert-small-v2")

    res = await model.embed(input_data)
    assert res.shape[0] == len(input_data)


@pytest.mark.asyncio
async def test_sentence_transformer_polars() -> None:
    model = SentenceTransformer("sentence-transformers/paraphrase-albert-small-v2")
    df = pl.DataFrame({input_column: input_data})

    res = await model.embed(df, input_column, output_column="embedding", model_version_column="version")

    assert res.height == df.height
    assert res["embedding"].null_count() == 0
    assert (res["version"] == model.model_version()).all()


@pytest.mark.asyncio
async def test_sentence_transformer_pandas() -> None:
    model = SentenceTransformer("sentence-transformers/paraphrase-albert-small-v2")
    df = pd.DataFrame({input_column: input_data})

    res = await model.embed(df, input_column, output_column="embedding", model_version_column="version")

    assert res.shape[0] == df.shape[0]
    assert (res["version"] == model.model_version()).all()
