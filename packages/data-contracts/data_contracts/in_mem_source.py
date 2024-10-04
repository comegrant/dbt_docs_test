import uuid

import polars as pl
from aligned.data_file import DataFileReference, upsert_on_column
from aligned.data_source.batch_data_source import BatchDataSource, CustomMethodDataSource, data_for_request
from aligned.feature_source import WritableFeatureSource
from aligned.retrival_job import RetrivalJob, RetrivalRequest


class InMemorySource(BatchDataSource, DataFileReference, WritableFeatureSource):

    def __init__(self, data: pl.DataFrame) -> None:
        self.data = data
        self.job_key = str(uuid.uuid4())

    def job_group_key(self) -> str:
        return self.job_key

    async def to_lazy_polars(self) -> pl.LazyFrame:
        return self.data.lazy()

    async def insert(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        values = await job.to_polars()
        if not self.data.is_empty():
            self.data = self.data.vstack(values.select(self.data.columns))
        else:
            self.data = values

    async def upsert(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        values = await job.to_lazy_polars()

        self.data = upsert_on_column(
            sorted(request.entity_names),
            new_data=values,
            existing_data=self.data.lazy()
        ).collect()

    async def overwrite(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        self.data = await job.to_polars()

    async def write_polars(self, df: pl.LazyFrame) -> None:
        self.data = df.collect()

    def all_data(self, request: RetrivalRequest, limit: int | None = None) -> RetrivalJob:
        from aligned import CustomMethodDataSource

        async def all_data(request: RetrivalRequest, limit: int | None = None) -> pl.LazyFrame:

            full_df = self.data
            if limit:
                full_df = self.data.head(limit)

            join_columns = set(request.all_returned_columns) - set(full_df.columns)
            if not join_columns:
                return full_df.lazy()

            random_df = (await data_for_request(request, full_df.height)).lazy()
            return full_df.hstack(random_df.select(pl.col(join_columns)).collect()).lazy()


        return CustomMethodDataSource.from_methods(all_data=all_data).all_data(request, limit)

    @classmethod
    def multi_source_features_for(
        cls: type['InMemorySource'], facts: RetrivalJob, requests: list[tuple['InMemorySource', RetrivalRequest]]
    ) -> RetrivalJob:
        from aligned.local.job import FileFactualJob

        sources = {
            source.job_group_key() for source, _ in requests if isinstance(source, BatchDataSource)
        }
        if len(sources) != 1:
            raise NotImplementedError(
                f'Type: {cls} have not implemented how to load fact data with multiple sources.'
            )

        source, _ = requests[0]

        async def random_features_for(facts: RetrivalJob, request: RetrivalRequest) -> pl.LazyFrame:
            join_columns = set(request.all_returned_columns) - set(source.data.columns)
            if not join_columns:
                return source.data.lazy()

            random = (await data_for_request(request, source.data.height)).lazy()
            return source.data.hstack(random.select(pl.col(join_columns)).collect()).lazy()

        request = RetrivalRequest.unsafe_combine([request for _, request in requests])

        return FileFactualJob(
            CustomMethodDataSource.from_methods(
                features_for=random_features_for,
            ).features_for(facts, request),
            [request for _, request in requests],
            facts
        )

    @staticmethod
    def from_values(values: dict[str, object]) -> 'InMemorySource':
        return InMemorySource(pl.DataFrame(values))

    @staticmethod
    def empty() -> 'InMemorySource':
        return InMemorySource.from_values({})
