import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, Protocol, TypeVar

from aligned.streams.interface import SinakableStream
from aligned.streams.redis import RedisStream
from azure.servicebus import (
    ServiceBusClient,
    ServiceBusReceivedMessage,
    ServiceBusReceiver,
    ServiceBusSender,
    ServiceBusSubQueue,
)
from azure.servicebus._common.message import PrimitiveTypes
from azure.servicebus.exceptions import MessageAlreadySettled, SessionLockLostError
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


class WritableStream(Protocol):
    async def write(self, data: BaseModel) -> None:
        ...

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        ...


@dataclass
class ServiceBusStreamWriter(WritableStream):
    client: ServiceBusClient
    topic_name: str
    application_properties: dict[str | bytes, PrimitiveTypes] = field(
        default_factory=dict
    )

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

        logger.info(f"Writing {len(data)} to {self.topic_name}")
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


@dataclass
class ServiceBusStream(Generic[T], ReadableStream[T]):
    payload: type[T]
    client: ServiceBusClient
    topic_name: str
    subscription_name: str
    sub_queue: ServiceBusSubQueue | None = field(default=None)
    default_max_records: int = field(default=10)
    max_wait_time: int = field(default=5)

    _connection: ServiceBusReceiver | None = field(default=None)

    def receiver(self) -> ServiceBusReceiver:
        if self._connection is None:
            rec = self.client.get_subscription_receiver(
                topic_name=self.topic_name,
                subscription_name=self.subscription_name,
                sub_queue=self.sub_queue,
                max_wait_time=self.max_wait_time,
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

        return messages

    async def mark_as_complete(self, messages: list[StreamMessage[T]]) -> None:
        if not messages:
            return

        receiver = self.receiver()

        logger.info(f"Marking {len(messages)} as completed for {self.topic_name} and {self.subscription_name}")

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


@dataclass
class PreselectorResultWriter(WritableStream):
    company_id: str

    async def write(self, data: BaseModel) -> None:
        await self.batch_write([data])

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        import polars as pl
        from data_contracts.preselector.store import Preselector

        df = pl.DataFrame(
            [
                row.model_dump()
                for row in data
                if isinstance(row, PreselectorSuccessfulResponse)
            ]
        )

        if df.is_empty():
            return

        df = (
            df.explode("year_weeks")
            .unnest("year_weeks")
            .with_columns(company_id=pl.lit(self.company_id))
        )

        await Preselector.query().store.insert_into(
            next(iter(Preselector.as_source().location_id())), df
        )

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

        await source.client.ping()

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

@dataclass
class MultipleWriter(WritableStream):

    sources: list[WritableStream]

    async def write(self, data: BaseModel) -> None:
        await self.batch_write([data])

    async def batch_write(self, data: Sequence[BaseModel]) -> None:
        for source in self.sources:
            await source.batch_write(data)
