from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import ceil
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import numpy as np

from embeddings.interface import Embedder
from embeddings.lazy_imports import (
    polars as pl,
)

if TYPE_CHECKING:
    from openai import AsyncClient

logger = logging.getLogger(__name__)


@dataclass
class OpenAI(Embedder):
    embedding_model: str = field(default="text-embedding-3-small")
    api_key: str = field(default_factory=lambda: environ["OPENAI_KEY"])

    def client(self) -> AsyncClient:
        from openai import AsyncClient

        return AsyncClient(api_key=self.api_key)

    def model_version(self) -> str:
        return self.embedding_model

    async def embed_numpy(self, data: np.ndarray) -> np.ndarray:
        return np.array(
            await embed_texts(
                data.tolist(),  # type: ignore
                model=self.embedding_model,
                skip_if_n_chunks=None,
                client=self.client(),
            )
        )


async def embed_texts(
    texts: list[str], model: str, skip_if_n_chunks: int | None, client: AsyncClient
) -> list[list[float]] | str:
    import tiktoken

    max_token_size = 8192
    number_of_texts = len(texts)

    chunks: list[int] = []
    chunk_size = 0
    encoder = tiktoken.encoding_for_model(model)

    for index, text in enumerate(texts):
        token_size = len(encoder.encode(text))

        if chunk_size + token_size > max_token_size:
            chunks.append(index)
            chunk_size = 0

            if skip_if_n_chunks and len(chunks) + 1 >= skip_if_n_chunks:
                return f"At text nr: {index} did it go above {skip_if_n_chunks} with {len(chunks)}"

        chunk_size += token_size

    if not chunks or number_of_texts - 1 > chunks[-1]:
        chunks.append(number_of_texts - 1)

    embeddings: list[list[float]] = []

    last_chunk_index = 0

    for chunk_index in chunks:
        if last_chunk_index == 0 and chunk_index >= number_of_texts - 1:
            chunk_texts = texts
        elif last_chunk_index == 0:
            chunk_texts = texts[:chunk_index]
        elif chunk_index >= number_of_texts - 1:
            chunk_texts = texts[last_chunk_index:]
        else:
            chunk_texts = texts[last_chunk_index:chunk_index]

        res = await client.embeddings.create(input=chunk_texts, model=model)
        embeddings.extend([emb.embedding for emb in res.data])
        last_chunk_index = chunk_index

    return embeddings


async def chunk_batch_embedding_request(texts: list[str], model: str, client: AsyncClient) -> pl.DataFrame:
    max_batch = 50_000
    number_of_batches = ceil(len(texts) / max_batch)

    batch_result: pl.DataFrame | None = None

    for i in range(number_of_batches):
        start = i * max_batch
        end_batch = min((i + 1) * max_batch, len(texts))

        if start == end_batch:
            batch_prompts = [texts[start]]
        else:
            batch_prompts = texts[start:end_batch]

        result = await make_batch_embedding_request(batch_prompts, model, client)

        if batch_result is None:
            batch_result = result
        else:
            batch_result = batch_result.hstack(result)

    assert batch_result is not None
    return batch_result


async def make_batch_embedding_request(texts: list[str], model: str, client: AsyncClient) -> pl.DataFrame:
    id_path = str(uuid4())
    batch_file = Path(id_path)
    output_file = Path(id_path + "-output.jsonl")

    write_batch_request(texts, batch_file, model, "/v1/embeddings")
    request_file = await client.files.create(file=batch_file, purpose="batch")
    response = await client.batches.create(
        input_file_id=request_file.id,
        endpoint="/v1/embeddings",
        completion_window="24h",
        metadata={"description": "Embedding batch job"},
    )
    status_response = await client.batches.retrieve(response.id)

    last_process = None
    expected_duration_left = 60

    while status_response.status not in ["completed", "failed"]:
        await asyncio.sleep(expected_duration_left * 0.8)  # Poll every minute
        status_response = await client.batches.retrieve(response.id)
        logger.info(f"Status of batch request {status_response.status}")

        processed_records = 0
        leftover_records = 0

        if status_response.request_counts:
            processed_records = status_response.request_counts.completed
            leftover_records = status_response.request_counts.total - processed_records

        if status_response.in_progress_at:
            last_process = datetime.fromtimestamp(status_response.in_progress_at, tz=timezone.utc)
            now = datetime.now(tz=timezone.utc)

            items_per_process = (now - last_process).total_seconds() / max(processed_records, 1)
            expected_duration_left = max(items_per_process * leftover_records, 60)

    batch_info = await client.batches.retrieve(response.id)
    output_file_id = batch_info.output_file_id

    if not output_file_id:
        raise ValueError(f"No output file for request: {response.id}")

    output_content = await client.files.retrieve_content(output_file_id)
    output_file.write_text(output_content)
    embeddings = pl.read_ndjson(output_file.as_posix())
    expanded_emb = (
        embeddings.unnest("response")
        .unnest("body")
        .explode("data")
        .select(["custom_id", "data"])
        .unnest("data")
        .select(["custom_id", "embedding"])
        .with_columns(pl.col("custom_id").str.split("-").list.get(1).alias("index"))
    )
    return expanded_emb


def write_batch_request(texts: list[str], path: Path, model: str, url: str) -> None:
    """
    Creates a .jsonl file for batch processing, with each line being a request to the embeddings API.
    """
    with path.open("w") as f:
        for i, text in enumerate(texts):
            request = {
                "custom_id": f"request-{i + 1}",
                "method": "POST",
                "url": url,
                "body": {"model": model, "input": text},
            }
            f.write(json.dumps(request) + "\n")
