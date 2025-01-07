import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, Protocol, TypeVar

from aligned import ContractStore
from aligned.feature_source import BatchDataSource
from aligned.streams.interface import SinakableStream
from aligned.streams.redis import RedisStream
from azure.servicebus import (
    AutoLockRenewer,
    ServiceBusClient,
    ServiceBusReceivedMessage,
    ServiceBusReceiver,
    ServiceBusSender,
    ServiceBusSubQueue,
)
from azure.servicebus._common.message import PrimitiveTypes
from azure.servicebus.exceptions import MessageAlreadySettled, SessionLockLostError
from data_contracts.preselector.basket_features import BasketFeatures
from data_contracts.preselector.store import Preselector
from data_contracts.sql_server import SqlServerConfig
from pydantic import BaseModel, ValidationError

from preselector.data.models.customer import PreselectorSuccessfulResponse

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


@dataclass
class StreamMessage(Generic[T]):
    body: T
    raw_message: Any


class ReadableStream(Protocol[T]):
    async def read(
        self, number_of_records: int | None = None
    ) -> list[StreamMessage[T]]:
        ...

    async def mark_as_complete(self, messages: list[StreamMessage[T]]) -> None:
        ...

    async def mark_as_uncomplete(self, messages: list[StreamMessage[T]]) -> None:
        ...


class WritableStream(Protocol):
    async def write(self, data: BaseModel) -> None:
        ...

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        ...

    def reader(self, model: type[T]) -> ReadableStream[T] | None:
        ...

@dataclass
class ServiceBusStreamWriter(WritableStream):
    client: ServiceBusClient
    topic_name: str
    application_properties: dict[str | bytes, PrimitiveTypes] = field(
        default_factory=dict
    )
    reader_subscription: str | None = field(default=None)

    _connection: ServiceBusSender | None = field(default=None)

    def sender(self) -> ServiceBusSender:
        if self._connection:
            return self._connection
        client = self.client.get_topic_sender(self.topic_name)
        self._connection = client.__enter__()
        return self._connection

    async def write(self, data: BaseModel) -> None:
        from azure.servicebus import ServiceBusMessage

        logger.info(
            f"Writing to service bus {self.topic_name} with {self.application_properties}"
        )
        self.sender().send_messages(
            ServiceBusMessage(
                data.model_dump_json(),
                application_properties=self.application_properties,
            )
        )

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        from azure.servicebus import ServiceBusMessage

        if not data:
            return

        logger.info(f"Writing {len(data)} to '{self.topic_name}'")
        sender = self.sender()
        batch = sender.create_message_batch()
        for item in data:
            batch.add_message(
                ServiceBusMessage(
                    item.model_dump_json(),
                    application_properties=self.application_properties,
                )
            )
        sender.send_messages(batch)

    def reader(self, model: type[T]) -> ReadableStream[T] | None:
        if self.reader_subscription is None:
            return None

        return ServiceBusStream(
            payload=model,
            client=self.client,
            topic_name=self.topic_name,
            subscription_name=self.reader_subscription,
            sub_queue=None
        )


@dataclass
class ServiceBusStream(Generic[T], ReadableStream[T]):
    payload: type[T]
    client: ServiceBusClient
    topic_name: str
    subscription_name: str
    sub_queue: ServiceBusSubQueue | None = field(default=None)
    default_max_records: int = field(default=10)
    max_wait_time: float | None = field(default=None)

    _connection: ServiceBusReceiver | None = field(default=None)

    def receiver(self) -> ServiceBusReceiver:
        if self._connection is None:
            renewer = AutoLockRenewer()
            rec = self.client.get_subscription_receiver(
                topic_name=self.topic_name,
                subscription_name=self.subscription_name,
                sub_queue=self.sub_queue,
                max_wait_time=self.max_wait_time,
                auto_lock_renewer=renewer,
                prefetch_count=self.default_max_records
            )
            self._connection = rec.__enter__()

        return self._connection

    async def read(
        self, number_of_records: int | None = None
    ) -> list[StreamMessage[T]]:
        receiver = self.receiver()
        raw_messages = receiver.receive_messages(
            max_message_count=number_of_records or self.default_max_records,
            max_wait_time=self.max_wait_time,
        )

        messages: list[StreamMessage[T]] = []
        for msg in raw_messages:
            body = list(msg.body)
            assert (
                len(body) == 1
            ), f"Expected only one message in the body, got {len(body)}"

            try:
                decoded = self.payload.model_validate_json(body[0])
                messages.append(
                    StreamMessage(
                        body=decoded, raw_message=msg
                    )
                )
            except ValidationError as error:
                logger.error(f"Unable to decode {body} - {error}")
                receiver.abandon_message(msg)

        logger.debug(f"Read {len(messages)} from service bus")
        return messages

    async def mark_as_complete(self, messages: list[StreamMessage[T]]) -> None:
        if not messages:
            return

        receiver = self.receiver()

        logger.info(f"Marking {len(messages)} as completed for '{self.topic_name}' and '{self.subscription_name}'")

        for message in messages:
            try:
                if isinstance(message.raw_message, ServiceBusReceivedMessage):
                    receiver.complete_message(message.raw_message)
                else:
                    message_type = type(message.raw_message)
                    logger.error(
                        f"Message is not of type ServiceBusReceivedMessage - got {message_type}"
                    )
            except (SessionLockLostError, MessageAlreadySettled) as error:
                logger.error(f"Error when marking message. Will try to abandon {error}")
                receiver.abandon_message(message.raw_message)

    async def mark_as_uncomplete(self, messages: list[StreamMessage[T]]) -> None:
        receiver = self.receiver()
        for message in messages:
            receiver.abandon_message(message.raw_message)


@dataclass
class StreamlitStreamMock(Generic[T], ReadableStream[T]):
    payload: type[T]
    index: int = field(default=0)

    async def read(self, number_of_records: int | None = None) -> list[StreamMessage[T]]:
        import streamlit as st
        from pydantic_form import pydantic_form

        data = pydantic_form(
            key=f"{self.payload.__name__}-{self.index}",
            model=self.payload,
            wait_for_submit=True,
        )
        st.write(data)
        self.index += 1

        if data is None:
            return []
        else:
            return [StreamMessage(data, self.payload)]

    async def mark_as_complete(self, messages: list[StreamMessage[T]]) -> None:
        pass

    async def mark_as_uncomplete(self, messages: list[StreamMessage[T]]) -> None:
        pass


@dataclass
class CustomWriter(WritableStream):

    function: Callable[[Sequence[BaseModel]], None]

    async def write(self, data: BaseModel) -> None:
        self.function([data])

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        self.function(data)

    def reader(self, model: type[T]) -> ReadableStream[T] | None:
        return None

@dataclass
class LoggerWriter(WritableStream):
    level: Literal["info", "error"] = field(default="info")
    context_title: str | None = field(default=None)

    async def write(self, data: BaseModel) -> None:
        log_func = logger.info
        if self.level == "error":
            log_func = logger.error

        if self.context_title:
            log_func(data)
        log_func(data)

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        log_func = logger.info
        if self.level == "error":
            log_func = logger.error

        if self.context_title:
            log_func(data)
        log_func(data)

    def reader(self, model: type[T]) -> ReadableStream[T] | None:
        return None

@dataclass
class StreamlitWriter(WritableStream):
    async def write(self, data: BaseModel) -> None:
        import streamlit as st

        st.write(data)

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        import streamlit as st

        if not data:
            st.write("No data to write")
            return

        st.write(data[:5])

    def reader(self, model: type[T]) -> ReadableStream[T] | None:
        return None



@dataclass
class SqlServerStream(Generic[T], ReadableStream[T]):
    payload: Callable[..., T]
    config: SqlServerConfig
    sql: str

    async def read(
        self, number_of_records: int | None = None
    ) -> list[StreamMessage[T]]:
        job = self.config.fetch(self.sql)
        df = await job.to_polars()

        return [
            StreamMessage(body=self.payload(**record), raw_message=None)
            for record in df.to_dicts()
        ]

    async def mark_as_complete(self, messages: list[StreamMessage[T]]) -> None:
        pass

    async def mark_as_uncomplete(self, messages: list[StreamMessage[T]]) -> None:
        pass



@dataclass
class PreselectorResultWriter(WritableStream):

    company_id: str

    store: ContractStore = field(default_factory=lambda: Preselector.query().store)
    sink: BatchDataSource | None = field(default=None)
    error_features: list[str] | None = field(default=None)

    async def write(self, data: BaseModel) -> None:
        await self.batch_write([data])

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        import polars as pl
        from aligned.schemas.feature import StaticFeatureTags

        if self.error_features is None:
            self.error_features = [
                feat.name for feat
                in BasketFeatures.query().request.all_returned_features
                if StaticFeatureTags.is_entity not in (feat.tags or [])
            ]

        assert self.error_features is not None

        mapped_data = [
            row.model_dump(
                exclude={
                    "year_weeks": {
                        "__all__":{
                            "ordered_weeks_ago",
                        }
                    }
                }
            )
            for row in data
            if isinstance(row, PreselectorSuccessfulResponse)
        ]

        year_week_struct = pl.List(pl.Struct({
            "year": pl.Int16,
            "week": pl.Int16,
            "variation_ids": pl.List(pl.String),
            "main_recipe_ids": pl.List(pl.Int32),
            "compliancy": pl.Int8,
            "target_cost_of_food_per_recipe": pl.Float32,
            "error_vector": pl.Struct({
                feat: pl.Float32
                for feat in self.error_features
            })
        }))
        df = pl.DataFrame(
            mapped_data,
            schema_overrides={
                "year_weeks": year_week_struct
            }
        )
        if df.is_empty():
            return

        df = (
            df.explode("year_weeks")
            .unnest("year_weeks")
            .with_columns(company_id=pl.lit(self.company_id))
        )

        request = self.store.feature_view(Preselector).request
        features = request.all_returned_columns

        if self.sink is not None:
            self.store = self.store.update_source_for(
                Preselector.location,
                self.sink
            )

        try:
            await self.store.upsert_into(
                Preselector.location, df.select(features)
            )
        except ValueError as e:
            logger.error(f"Error when upserting {df.head()}")
            logger.exception(e)

    def reader(self, model: type[T]) -> ReadableStream[T] | None:
        return None



@dataclass
class PreselectorResultStreamWriter(WritableStream):

    company_id: str
    timestamp_columns: list[str]
    stream: SinakableStream

    async def write(self, data: BaseModel) -> None:
        await self.batch_write([data])

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        if not data:
            return

        await self.stream.sink([
            { "json_data": row.model_dump_json() }
            for row in data
        ])

    @staticmethod
    async def for_company(company_id: str) -> 'PreselectorResultStreamWriter':
        from data_contracts.preselector.store import Preselector

        view = Preselector.query().view
        assert view.stream_data_source is not None
        source = view.stream_data_source.consumer()
        assert isinstance(source, RedisStream)

        await source.client.ping() # type: ignore

        assert isinstance(source, SinakableStream)

        result = view.request_all.request_result

        timestamps = []
        if result.event_timestamp:
            timestamps = [result.event_timestamp]

        return PreselectorResultStreamWriter(
            company_id=company_id,
            timestamp_columns=timestamps,
            stream=source
        )

    def reader(self, model: type[T]) -> ReadableStream[T] | None:
        return None


@dataclass
class MultipleWriter(WritableStream):

    sources: list[WritableStream]

    async def write(self, data: BaseModel) -> None:
        await self.batch_write([data])

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        for source in self.sources:
            await source.batch_write(data)

    def reader(self, model: type[T]) -> ReadableStream[T] | None:
        return None


@dataclass
class CustomReader(ReadableStream[T]):

    method: Callable[[int | None], Awaitable[list[T]]]

    async def read(self, number_of_records: int | None = None) -> list[StreamMessage[T]]:
        messages = await self.method(number_of_records)
        return [
            StreamMessage(msg, raw_message=None)
            for msg in messages
        ]

    async def mark_as_complete(self, messages: list[StreamMessage[T]]) -> None:
        pass

    async def mark_as_uncomplete(self, messages: list[StreamMessage[T]]) -> None:
        pass
