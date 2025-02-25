import pandas as pd
import polars as pl
import pytest
from embeddings import OpenAI
from openai.types import Embedding
from openai.types.create_embedding_response import CreateEmbeddingResponse, Usage
from pytest_mock import MockFixture

input_column = "text"
input_data = ["Hello world", "Welcome to the data kicthen"]

model = OpenAI(api_key="should not call the actual API")
dummy_response = CreateEmbeddingResponse(
    data=[
        Embedding(embedding=[1, 2, 3], index=0, object="embedding"),
        Embedding(embedding=[3, 2, 1], index=1, object="embedding"),
    ],
    model="",
    usage=Usage(prompt_tokens=10, total_tokens=10),
    object="list",
)


@pytest.mark.asyncio
async def test_openai_embeddings_list(mocker: MockFixture) -> None:
    mocker.patch("openai.resources.embeddings.AsyncEmbeddings.create", return_value=dummy_response)

    res = await model.embed(input_data)
    assert res.shape[0] == len(dummy_response.data)


@pytest.mark.asyncio
async def test_openai_embeddings_polars(mocker: MockFixture) -> None:
    df = pl.DataFrame({input_column: input_data})

    mocker.patch("openai.resources.embeddings.AsyncEmbeddings.create", return_value=dummy_response)
    res = await model.embed(df, input_column, output_column="embedding", model_version_column="version")

    assert res.height == len(dummy_response.data)
    assert res["embedding"].null_count() == 0
    assert (res["version"] == model.model_version()).all()


@pytest.mark.asyncio
async def test_openai_embeddings_pandas(mocker: MockFixture) -> None:
    df = pd.DataFrame({input_column: input_data})

    mocker.patch("openai.resources.embeddings.AsyncEmbeddings.create", return_value=dummy_response)
    res = await model.embed(df, input_column, output_column="embedding", model_version_column="version")

    assert res.shape[0] == len(dummy_response.data)
    assert (res["version"] == model.model_version()).all()
