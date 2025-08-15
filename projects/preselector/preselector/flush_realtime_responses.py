import logging

import polars as pl
from aligned import ContractStore
from azure.servicebus import ServiceBusSubQueue
from data_contracts.preselector.store import FailedPreselectorOutput, SuccessfulPreselectorOutput

from preselector.data.models.customer import (
    PreselectorFailedResponse,
    PreselectorSuccessfulResponse,
)
from preselector.process_stream_settings import ProcessStreamSettings
from preselector.realtime_streams import connect_to_streams
from preselector.store import preselector_store
from preselector.stream import ReadableStream, ServiceBusStream

logger = logging.getLogger(__name__)


async def flush_deadletter(settings: ProcessStreamSettings) -> None:
    streams = await connect_to_streams(settings, company_id="")

    if streams.successful_output_stream is None or streams.failed_output_stream is None:
        logger.info("There was no output sources. Will exit early")
        return

    success_stream = streams.successful_output_stream.reader(PreselectorSuccessfulResponse)
    failed_stream = streams.failed_output_stream.reader(PreselectorFailedResponse)

    if isinstance(success_stream, ServiceBusStream):
        success_stream.sub_queue = ServiceBusSubQueue.DEAD_LETTER
    if isinstance(failed_stream, ServiceBusStream):
        failed_stream.sub_queue = ServiceBusSubQueue.DEAD_LETTER

    await write_to_databricks(
        success_stream=success_stream,
        failed_stream=failed_stream,
        write_store=preselector_store(),
        write_size=settings.write_output_max_size,
        max_wait_time=settings.write_output_wait_time,
    )


async def write_to_databricks(
    success_stream: ReadableStream[PreselectorSuccessfulResponse] | None,
    failed_stream: ReadableStream[PreselectorFailedResponse] | None,
    write_store: ContractStore,
    write_size: int,
    max_wait_time: float | None,
) -> None:
    from pyspark.errors.exceptions.base import PySparkException

    if success_stream is None:
        logger.info("No readable success stream.")
        return

    if isinstance(success_stream, ServiceBusStream):
        success_stream.max_wait_time = max_wait_time

    message_count = 0
    success_messages = await success_stream.read(number_of_records=write_size)

    while success_messages and message_count < write_size:
        message_count += len(success_messages)
        try:
            df = pl.concat([output.body.to_dataframe() for output in success_messages], how="vertical_relaxed")

            await write_store.contract(SuccessfulPreselectorOutput).insert(df)
            await success_stream.mark_as_complete(success_messages)

            if message_count < write_size:
                success_messages = await success_stream.read(number_of_records=write_size)

        except (ValueError, PySparkException) as error:
            logger.exception(f"Unable to write to databricks {error}")
            await success_stream.mark_as_uncomplete(success_messages)
            message_count = write_size

    if failed_stream is None:
        logger.info("No readable success stream.")
        return

    if isinstance(failed_stream, ServiceBusStream):
        failed_stream.max_wait_time = max_wait_time

    message_count = 0
    failed_messages = await failed_stream.read(number_of_records=write_size)
    while failed_messages and message_count < write_size:
        try:
            await write_store.feature_view(FailedPreselectorOutput).insert(
                pl.concat([failed_req.body.to_dataframe() for failed_req in failed_messages], how="vertical_relaxed")
            )
            await failed_stream.mark_as_complete(failed_messages)

            if message_count < write_size:
                failed_messages = await failed_stream.read(number_of_records=write_size)
        except (ValueError, PySparkException) as error:
            logger.exception(f"Unable to write to databricks {error}")
            await success_stream.mark_as_uncomplete(success_messages)
            message_count = write_size
