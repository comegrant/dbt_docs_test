from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path

import pandas as pd
import polars as pl
from aligned.compiler.feature_factory import FeatureFactory
from aligned.data_source.batch_data_source import BatchDataSource, ColumnFeatureMappable
from aligned.exceptions import UnableToFindFileException
from aligned.feature_source import WritableFeatureSource
from aligned.retrival_job import RetrivalJob, RetrivalRequest
from aligned.sources.local import (
    CsvConfig,
    CsvFileSource,
    DataFileReference,
    FileSource,
    ParquetConfig,
    ParquetFileSource,
    StorageFileReference,
)
from aligned.storage import Storage
from azure.storage.blob import BlobServiceClient
from httpx import HTTPStatusError


@dataclass
class AzurePath:
    container: str
    blob_path: str


def azure_container_blob(path: str) -> AzurePath:
    splits = path.split("/")
    return AzurePath(container=splits[0], blob_path="/".join(splits[1:]))


@dataclass
class LocalFolder:
    folder: str

    def parquet_at(self, path: str) -> ParquetFileSource:
        return FileSource.parquet_at(f"{self.folder}/{path}")

    def csv_at(self, path: str) -> CsvFileSource:
        return FileSource.csv_at(f"{self.folder}/{path}")


@dataclass
class AzureBlobConfig:
    account_id_env: str
    tenent_id_env: str
    client_id_env: str
    client_secret_env: str
    account_name_env: str

    def parquet_at(self, path: str) -> AzureBlobParquetDataSource:
        return AzureBlobParquetDataSource(self, path)

    def csv_at(self, path: str) -> AzureBlobCsvDataSource:
        return AzureBlobCsvDataSource(self, path)

    def delta_at(
        self, path: str, mapping_keys: dict[str, str] | None = None
    ) -> AzureBlobDeltaDataSource:
        return AzureBlobDeltaDataSource(self, path, mapping_keys=mapping_keys or {})

    def directory(self, path: str) -> AzureBlobDirectory:
        return AzureBlobDirectory(self, Path(path))

    def client(self) -> BlobServiceClient:
        creds = self.read_creds()
        account_name = creds["account_name"]
        account_url = f"https://{account_name}.blob.core.windows.net/"

        if "account_key" in creds:
            return BlobServiceClient(account_url=account_url, credential=creds)
        else:
            from azure.identity import ClientSecretCredential

            creds = ClientSecretCredential(
                tenant_id=creds["tenant_id"],
                client_id=creds["client_id"],
                client_secret=creds["client_secret"],
            )

            return BlobServiceClient(account_url=account_url, credential=creds)

    def read_creds(self) -> dict[str, str]:
        import os

        account_name = os.environ[self.account_name_env]

        if self.account_id_env in os.environ:
            return {
                "account_name": account_name,
                "account_key": os.environ[self.account_id_env],
            }
        else:
            return {
                "account_name": account_name,
                "tenant_id": os.environ[self.tenent_id_env],
                "client_id": os.environ[self.client_id_env],
                "client_secret": os.environ[self.client_secret_env],
            }

    @property
    def storage(self) -> BlobStorage:
        return BlobStorage(self)


@dataclass
class AzureBlobDirectory:
    config: AzureBlobConfig
    sub_path: Path

    def parquet_at(self, path: str) -> AzureBlobParquetDataSource:
        sub_path = self.sub_path / path
        return self.config.parquet_at(sub_path.as_posix())

    def csv_at(self, path: str) -> AzureBlobCsvDataSource:
        sub_path = self.sub_path / path
        return self.config.csv_at(sub_path.as_posix())

    def delta_at(
        self, path: str, mapping_keys: dict[str, str] | None = None
    ) -> AzureBlobDeltaDataSource:
        sub_path = self.sub_path / path
        return self.config.delta_at(sub_path.as_posix(), mapping_keys)

    def sub_dir(self, path: str) -> AzureBlobDirectory:
        return AzureBlobDirectory(self.config, self.sub_path / path)


@dataclass
class BlobStorage(Storage):
    config: AzureBlobConfig

    async def read(self, path: str) -> bytes:
        azure_path = azure_container_blob(path)
        client = self.config.client()
        container = client.get_blob_client(azure_path.container, azure_path.blob_path)

        with BytesIO() as byte_stream:
            container.download_blob().download_to_stream(byte_stream)
            byte_stream.seek(0)
            return byte_stream.read()

    async def write(self, path: str, content: bytes) -> None:
        azure_path = azure_container_blob(path)
        client = self.config.client()
        container = client.get_blob_client(azure_path.container, azure_path.blob_path)
        container.upload_blob(content, overwrite=True)


@dataclass
class AzureBlobDataSource(StorageFileReference, ColumnFeatureMappable):
    config: AzureBlobConfig
    path: str

    type_name: str = "azure_blob"

    def job_group_key(self) -> str:
        return f"{self.type_name}/{self.path}"

    @property
    def storage(self) -> Storage:
        return self.config.storage

    async def read(self) -> bytes:
        return await self.storage.read(self.path)

    async def write(self, content: bytes) -> None:
        return await self.storage.write(self.path, content)


@dataclass
class AzureBlobCsvDataSource(
    BatchDataSource, DataFileReference, ColumnFeatureMappable
):
    config: AzureBlobConfig
    path: str
    mapping_keys: dict[str, str] = field(default_factory=dict)
    csv_config: CsvConfig = field(default_factory=CsvConfig)

    type_name: str = "azure_blob_csv"

    def as_markdown(self) -> str:
        return f"""Type: *Azure Blob Csv File*

Path: *{self.path}*
"""

    def job_group_key(self) -> str:
        return f"{self.type_name}/{self.path}"

    @property
    def storage(self) -> Storage:
        return self.config.storage

    async def to_polars(self) -> pl.LazyFrame:
        url = f"az://{self.path}"
        return pl.read_csv(
            url,
            separator=self.csv_config.seperator,
            storage_options=self.config.read_creds(),
        ).lazy()

    async def to_pandas(self) -> pd.DataFrame:
        try:
            data = await self.storage.read(self.path)
            buffer = BytesIO(data)
            return pd.read_csv(
                buffer,
                sep=self.csv_config.seperator,
                compression=self.csv_config.compression,
            )
        except FileNotFoundError:
            raise UnableToFindFileException()
        except HTTPStatusError:
            raise UnableToFindFileException()

    async def write_pandas(self, df: pd.DataFrame) -> None:
        url = f"az://{self.path}"
        df.to_csv(
            url,
            sep=self.csv_config.seperator,
            compression=self.csv_config.compression,
            storage_options=self.config.read_creds(),
        )

    async def write_polars(self, df: pl.LazyFrame) -> None:
        await self.write_pandas(df.collect().to_pandas())

    async def write(self, job: RetrivalJob, requests: list[RetrivalRequest]) -> None:
        if len(requests) != 1:
            raise ValueError(f"Only support writing on request, got {len(requests)}.")

        features = requests[0].all_returned_columns
        df = await job.to_polars()
        await self.write_polars(df.select(features))


@dataclass
class AzureBlobParquetDataSource(
    BatchDataSource, DataFileReference, ColumnFeatureMappable
):
    config: AzureBlobConfig
    path: str
    mapping_keys: dict[str, str] = field(default_factory=dict)
    parquet_config: ParquetConfig = field(default_factory=ParquetConfig)
    type_name: str = "azure_blob_parquet"

    def as_markdown(self) -> str:
        return f"""Type: *Azure Blob Parquet File*

Path: *{self.path}*"""

    def job_group_key(self) -> str:
        return f"{self.type_name}/{self.path}"

    def __hash__(self) -> int:
        return hash(self.job_group_key())

    @property
    def storage(self) -> Storage:
        return self.config.storage

    async def read_pandas(self) -> pd.DataFrame:
        try:
            data = await self.storage.read(self.path)
            buffer = BytesIO(data)
            return pd.read_parquet(buffer)
        except FileNotFoundError:
            raise UnableToFindFileException(self.path)
        except HTTPStatusError:
            raise UnableToFindFileException(self.path)

    async def to_polars(self) -> pl.LazyFrame:
        try:
            url = f"az://{self.path}"
            creds = self.config.read_creds()
            return pl.scan_parquet(url, storage_options=creds)
        except FileNotFoundError:
            raise UnableToFindFileException(self.path)
        except HTTPStatusError:
            raise UnableToFindFileException(self.path)

    async def write_pandas(self, df: pd.DataFrame) -> None:
        buffer = BytesIO()
        df.to_parquet(
            buffer,
            compression=self.parquet_config.compression,
            engine=self.parquet_config.engine,
        )
        buffer.seek(0)
        await self.storage.write(self.path, buffer.read())

    async def write_polars(self, df: pl.LazyFrame) -> None:
        url = f"az://{self.path}"
        creds = self.config.read_creds()
        df.collect().to_pandas().to_parquet(url, storage_options=creds)


    async def schema(self) -> dict[str, FeatureFactory]:
        from aligned.schemas.feature import FeatureType
        df = await self.to_polars()
        parquet_schema = df.schema
        return {
            name: FeatureType.from_polars(pl_type).feature_factory for name, pl_type in parquet_schema.items()
        }


@dataclass
class AzureBlobDeltaDataSource(
    BatchDataSource,
    DataFileReference,
    ColumnFeatureMappable,
    WritableFeatureSource,
):
    config: AzureBlobConfig
    path: str
    mapping_keys: dict[str, str] = field(default_factory=dict)
    type_name: str = "azure_blob_delta"

    def job_group_key(self) -> str:
        return f"{self.type_name}/{self.path}"

    def as_markdown(self) -> str:
        return f"""Type: Azure Blob Delta File

Path: *{self.path}*"""

    @property
    def storage(self) -> Storage:
        return self.config.storage

    async def read_pandas(self) -> pd.DataFrame:
        return (await self.to_polars()).collect().to_pandas()

    async def to_polars(self) -> pl.LazyFrame:
        try:
            url = f"az://{self.path}"
            creds = self.config.read_creds()
            return pl.scan_delta(url, storage_options=creds)
        except FileNotFoundError:
            raise UnableToFindFileException()
        except HTTPStatusError:
            raise UnableToFindFileException()

    async def write_pandas(self, df: pd.DataFrame) -> None:
        await self.write_polars(pl.from_pandas(df).lazy())

    async def write_polars(self, df: pl.LazyFrame) -> None:
        url = f"az://{self.path}"
        creds = self.config.read_creds()

        df.collect().write_delta(
            url,
            storage_options=creds,
            mode="append",
        )
