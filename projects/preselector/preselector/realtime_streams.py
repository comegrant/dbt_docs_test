import logging
from dataclasses import dataclass

from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusSubQueue

from preselector.process_stream_settings import ProcessStreamSettings
from preselector.schemas.batch_request import GenerateMealkitRequest
from preselector.stream import (
    ReadableStream,
    ServiceBusStream,
    ServiceBusStreamWriter,
    StreamlitStreamMock,
    StreamlitWriter,
    WritableStream,
)

logger = logging.getLogger(__name__)

@dataclass
class PreselectorStreams:
    request_stream: ReadableStream[GenerateMealkitRequest]

    successful_output_stream: WritableStream | None
    failed_output_stream: WritableStream | None


async def connect_to_streams(settings: ProcessStreamSettings, company_id: str) -> PreselectorStreams:
    if settings.service_bus_connection_string:
        client = ServiceBusClient.from_connection_string(
            settings.service_bus_connection_string.get_secret_value()
        )
    elif settings.service_bus_namespace:
        client = ServiceBusClient(
            fully_qualified_namespace=settings.service_bus_namespace,
            credential=DefaultAzureCredential()
        )
    else:
        logger.info("No connection string set, using Streamlit mock")
        return PreselectorStreams(
            request_stream=StreamlitStreamMock(GenerateMealkitRequest),
            successful_output_stream=StreamlitWriter(),
            failed_output_stream=StreamlitWriter(),
        )

    assert (
        settings.service_bus_subscription_name is not None
    ), "Subscription name is required when a connection string is set"

    sub_queue = ServiceBusSubQueue(settings.service_bus_sub_queue) if settings.service_bus_sub_queue else None
    request_stream = ServiceBusStream(
        payload=GenerateMealkitRequest,
        client=client,
        topic_name=settings.service_bus_request_topic_name,
        subscription_name=settings.service_bus_subscription_name,
        sub_queue=sub_queue,
        default_max_records=settings.service_bus_request_size,
        max_wait_time=settings.ideal_poll_interval
    )

    error_writer = ServiceBusStreamWriter(
        client,
        topic_name=settings.service_bus_failed_topic_name,
        application_properties={"company": settings.service_bus_subscription_name},
        reader_subscription=settings.service_bus_failed_subscription_name,
        reader_subqueue=sub_queue
    )
    success_writer = ServiceBusStreamWriter(
        client,
        topic_name=settings.service_bus_success_topic_name,
        application_properties={"company": settings.service_bus_subscription_name},
        reader_subscription=settings.service_bus_success_subscription_name,
        reader_subqueue=sub_queue
    )

    return PreselectorStreams(
        request_stream=request_stream,
        successful_output_stream=success_writer
        if settings.service_bus_should_write
        else None,
        failed_output_stream=error_writer
        if settings.service_bus_should_write
        else None,
    )
