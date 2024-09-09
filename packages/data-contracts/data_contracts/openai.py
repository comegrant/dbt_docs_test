import asyncio
import json
import logging
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import polars as pl
from aligned.exposed_model.interface import (
    ExposedModel,
    Feature,
    FeatureReference,
    RetrivalJob,
)
from aligned.feature_store import ModelFeatureStore
from httpx import HTTPError

logger = logging.getLogger(__name__)


@dataclass
class OpenAiCompletion(ExposedModel):
    model_name: str

    system_message: str | None
    prompt_template: str
    output_name: str

    input_features_versions: str | None = None

    model_type: str = "openai"

    @property
    def exposed_at_url(self) -> str | None:
        return "https://api.openai.com/"

    def prompt_template_hash(self) -> str:
        from hashlib import sha256

        return sha256(self.prompt_template.encode(), usedforsecurity=False).hexdigest()

    @property
    def as_markdown(self) -> str:
        return f"""Sending a `chat/completion` request to OpenAI's API.

This will use the model: `{self.model_name}` to generate the responses.

And use the prompt template:
```
{self.prompt_template}
```"""

    async def needed_features(self, store: ModelFeatureStore) -> list[FeatureReference]:
        return list(store.model.feature_references(self.input_features_versions))

    async def needed_entities(self, store: ModelFeatureStore) -> set[Feature]:
        return store.needed_entities()

    async def run_polars(
        self, values: RetrivalJob, store: ModelFeatureStore
    ) -> pl.DataFrame:
        import os

        from openai import AsyncClient

        client = AsyncClient(api_key=os.environ["OPENAI_TOKEN"])

        model_version_column = store.model.predictions_view.model_version_column

        entity_info = list(await self.needed_entities(store))
        entities = await values.to_polars()

        features = await self.needed_features(store)
        expected_cols = {feat.name for feat in features}
        missing_cols = expected_cols - set(entities.columns)

        if missing_cols:
            entities = await store.features_for(values).to_polars()

        prompts = entities

        model_version = f"{self.prompt_template_hash()} -> {self.model_name}"

        def request_for_prompt(prompt: str, entity_key: str) -> str:
            system_message = self.system_message or "You are a helpful assistant."

            return json.dumps(
                {
                    "custom_id": entity_key,
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": "gpt-3.5-turbo",
                        "temperature": 0.1,
                        "messages": [
                            {"role": "system", "content": system_message},
                            {"role": "user", "content": prompt},
                        ],
                    },
                }
            )

        if entities.is_empty():
            raise ValueError(
                f"No prompts to generate completions for. {entities} is empty"
            )

        file_name = f"{uuid4()}.jsonl"
        with Path(file_name).open(mode="a") as file:
            for value in prompts.iter_rows(named=True):
                prompt = self.prompt_template.format(**value)

                entity_key = "-".join([str(value[col.name]) for col in entity_info])

                file.write(request_for_prompt(prompt, entity_key))
                file.write("\n")

        with Path(file_name).open("rb") as open_file:
            file = await client.files.create(
                file=open_file,
                purpose="batch",
            )

        batch_request = await client.batches.create(
            input_file_id=file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={"description": "Generate completions for recipe prompts."},
        )
        logger.info(f"Created batch request: {batch_request.id}")

        running_statuses = {"in_progress", "validating", "finalizing"}
        status = "in_progress"

        while status in running_statuses:
            await asyncio.sleep(5)
            with suppress(HTTPError):
                batch_request = await client.batches.retrieve(batch_request.id)

            status = batch_request.status

        if batch_request.errors:
            logger.error(batch_request.errors)

        if batch_request.output_file_id:
            content = await client.files.content(batch_request.output_file_id)
        elif batch_request.error_file_id:
            content = await client.files.content(batch_request.error_file_id)
        else:
            if batch_request.errors:
                raise ValueError(
                    f"Batch request failed with errors: {batch_request.errors}"
                )

            raise ValueError("No output file found in batch request.")

        ret_vals: list[dict] = []

        responses = pl.read_ndjson(content.content)
        for response in responses.iter_rows(named=True):
            key = response["custom_id"]

            sub_res = {}

            for entity, value in zip(entity_info, key.split("-"), strict=False):
                sub_res[entity.name] = entity.dtype.python_type(value)

            sub_res[self.output_name] = response["response"]["body"]["choices"][0][
                "message"
            ]["content"]
            ret_vals.append(sub_res)

        ret_df = pl.DataFrame(ret_vals)

        for entity in entity_info:
            ret_df = ret_df.with_columns(
                pl.col(entity.name).cast(entity.dtype.polars_type)
            )

        if model_version_column:
            ret_df = ret_df.with_columns(
                pl.lit(model_version).alias(model_version_column.name)
            )

        return prompts.join(ret_df, on="recipe_id", how="left")
