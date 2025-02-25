from __future__ import annotations

from typing import Union, overload

import numpy as np

from embeddings.lazy_imports import (
    pandas as pd,
)
from embeddings.lazy_imports import (
    polars as pl,
)

DataFrameType = Union[pl.DataFrame, pd.DataFrame, pl.LazyFrame, list[str], np.ndarray]


class Embedder:
    @overload
    async def embed(
        self,
        data: list[str],
        input_column: str | None = None,
        output_column: str | None = None,
        model_version_column: str | None = None,
    ) -> np.ndarray: ...

    @overload
    async def embed(
        self,
        data: np.ndarray,
        input_column: str | None = None,
        output_column: str | None = None,
        model_version_column: str | None = None,
    ) -> np.ndarray: ...

    @overload
    async def embed(
        self,
        data: pd.DataFrame,
        input_column: str | None,
        output_column: str | None = None,
        model_version_column: str | None = None,
    ) -> pd.DataFrame: ...

    @overload
    async def embed(
        self,
        data: pl.DataFrame,
        input_column: str | None,
        output_column: str | None = None,
        model_version_column: str | None = None,
    ) -> pl.DataFrame: ...

    async def embed(
        self,
        data: DataFrameType,
        input_column: str | None = None,
        output_column: str | None = None,
        model_version_column: str | None = None,
    ) -> DataFrameType:
        """
        Embeds a text column and returns a new data frame with the embedded representation.

        Args:
            data (DataFrameType): The data frame that contains the texts to embed
            input_column (str): The text column to embed
            output_column (str | None): The column to store the embeddings in
            model_version_column (str | None): The column to store the model version in

        Returns:
            DataFrameType: A new data frame containing the embeddings and the model version

        ```python
        import polars as pl
        from embeddings import OpenAI, Embedder

        model: Embedder = OpenAI(api_key="...") # or define nothing as it will read the `OPENAI_KEY` env var.

        input_df = pl.DataFrame({
            "text": ["Hello", "world"]
        })
        embedded_df = await model.embed(input_df, input_column="text")

        print(embedded_df)
        ```
        """
        if output_column is None:
            output_column = f"embedded_{input_column}"

        if model_version_column is None:
            model_version_column = f"{output_column}_model_version"

        if isinstance(data, list):
            data = np.array(data)

        if isinstance(data, np.ndarray):
            return await self.embed_numpy(data)

        assert input_column, "Assumes that an input_column is defined if the data is a DataFrame"

        if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            if output_column in data.collect_schema():
                return data

            if isinstance(data, pl.LazyFrame):
                in_mem_data = data.collect()
            else:
                in_mem_data = data

            output = in_mem_data.with_columns(pl.lit(self.model_version()).alias(model_version_column)).hstack(
                [(await self.embed_polars(in_mem_data[input_column])).alias(output_column)]
            )

            if isinstance(data, pl.LazyFrame):
                return output.lazy()
            elif isinstance(data, pl.DataFrame):
                return output
        else:
            if output_column in data.columns:
                return data

            res_data = data.copy()
            res_data[model_version_column] = self.model_version()
            res_data[output_column] = await self.embed_pandas(data[input_column])  # type: ignore
            return res_data

    def model_version(self) -> str:
        raise NotImplementedError(type(self))

    async def embed_numpy(self, data: np.ndarray) -> np.ndarray:
        raise NotImplementedError(type(self))

    async def embed_polars(self, data: pl.Series) -> pl.Series:
        return pl.Series(
            values=await self.embed_numpy(data.to_numpy()),
            dtype=pl.List(pl.Float32()),
        )

    async def embed_pandas(self, data: pd.Series) -> pd.Series:
        embeds = await self.embed_numpy(data.to_numpy())
        return pd.Series(
            data=embeds.tolist(),
            dtype="object",
        )
