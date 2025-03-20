import asyncio
import logging
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone

import polars as pl
from aligned import ContractStore
from aligned.data_source.batch_data_source import BatchDataSource, DataFileReference
from aligned.feature_source import (
    FeatureLocation,
    WritableFeatureSource,
)
from aligned.sources.in_mem_source import InMemorySource
from azure.core.exceptions import ResourceNotFoundError
from azure.servicebus.exceptions import MessageAlreadySettled, SessionLockLostError
from cheffelo_logging import DataDogConfig
from cheffelo_logging.logging import DataDogStatsdConfig
from data_contracts.orders import WeeksSinceRecipe
from data_contracts.preselector.basket_features import PredefinedVectors
from data_contracts.recipe import RecipeEmbedding
from data_contracts.recommendations.recommendations import RecommendatedDish
from data_contracts.unity_catalog import DatabricksConnectionConfig, DatabricksSource
from pydantic import Field
from pydantic_settings import BaseSettings

from preselector.data.models.customer import (
    PreselectorFailedResponse,
    PreselectorSuccessfulResponse,
)
from preselector.flush_realtime_responses import write_to_databricks
from preselector.main import duration, run_preselector_for_request
from preselector.process_stream_settings import ProcessStreamSettings
from preselector.realtime_streams import connect_to_streams
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference
from preselector.store import preselector_store
from preselector.stream import (
    ReadableStream,
    StreamMessage,
    WritableStream,
)

logger = logging.getLogger(__name__)


class KeyVaultSettings(BaseSettings):
    secrets_url: str = Field("https://gg-dev-svc-bcp-qa.vault.azure.net/")
    key_mappings: dict[str, str] = Field({"service-bus-connection-string": "service_bus_connection_string".upper()})


async def load_cache_for(
    store: ContractStore, loads: list[tuple[FeatureLocation, BatchDataSource, pl.Expr | None]], force_load: bool = False
) -> ContractStore:
    for location, source, pl_filter in loads:
        if location.location_type == "feature_view":
            request = store.feature_view(location.name).request
        else:
            request = store.model(location.name).prediction_request()

        assert isinstance(
            source, (WritableFeatureSource, DataFileReference)
        ), f"Expected a writable source, got '{type(source)}'"

        if not force_load and (
            (isinstance(source, InMemorySource) and not source.data.is_empty())
            or (not isinstance(source, InMemorySource))
        ):
            try:
                # Checking if the data exists locally already with the correct schema
                _ = await source.all_data(request, limit=10).to_polars()
                store = store.update_source_for(location, source)
                logger.info(f"Found data for {location}, will therefore skip")
                continue
            except Exception:
                logger.info(f"Will load data due to either missing data, or incorrect schema. for {location}")

        from_source = store.sources[location]
        logger.info(f"Loading {location} to cache from {from_source.source_id()}")

        if location.location_type == "feature_view":
            job = store.feature_view(location.name).all().remove_derived_features()
        else:
            job = store.model(location.name).all_predictions().remove_derived_features()

        if pl_filter is not None:

            def filter_df(df: pl.LazyFrame) -> pl.LazyFrame:
                assert pl_filter is not None  # noqa: B023
                return df.filter(pl_filter)  # noqa: B023

            job = job.polars_method(filter_df)

        if isinstance(source, WritableFeatureSource):
            await source.overwrite(job, request)
        else:
            await job.write_to_source(source)

        store = store.update_source_for(location, source)
        logger.info(f"Loaded {location.name} from cache")

    return store


async def load_cache(
    store: ContractStore, company_id: str, exclude_views: set[FeatureLocation] | None = None, force_load: bool = False
) -> ContractStore:
    from preselector.recipe_contracts import cache_dir

    if exclude_views is None:
        exclude_views = set()

    today = date.today()
    five_weeks_into_the_future = today + timedelta(weeks=5)
    this_week = today.isocalendar().week
    this_year = today.isocalendar().year

    rec_partitions = (pl.col("year") >= this_year) & (pl.col("week") > this_week)

    if five_weeks_into_the_future.year != this_year:
        rec_partitions = rec_partitions | (pl.col("year") >= five_weeks_into_the_future.year)

    depends_on = store.feature_view("preselector_output").view.source.depends_on()

    partition_recs = cache_dir.partitioned_parquet_at(f"{company_id}/recs", partition_keys=["year", "week"])

    cache_sources: list[tuple[FeatureLocation, BatchDataSource, pl.Expr | None]] = [
        (
            FeatureLocation.feature_view("recipe_cost"),
            cache_dir.parquet_at("recipe_cost.parquet"),
            (pl.col("menu_year") >= this_year) & (pl.col("menu_week") > this_week),
        ),
        (
            FeatureLocation.feature_view("preselector_year_week_menu"),
            cache_dir.parquet_at(f"{company_id}/menus.parquet"),
            (pl.col("menu_year") >= this_year) & (pl.col("company_id") == company_id),
        ),
        (
            FeatureLocation.feature_view("normalized_recipe_features"),
            cache_dir.parquet_at(f"{company_id}/normalized_recipe_features"),
            (pl.col("year") >= this_year) & (pl.col("company_id") == company_id),
        ),
        (RecommendatedDish.location, partition_recs, (pl.col("company_id") == company_id) & rec_partitions),
        (PredefinedVectors.location, InMemorySource.empty(), pl.col("company_id") == company_id),
        (
            WeeksSinceRecipe.location,
            cache_dir.parquet_at(f"{company_id}/weeks_since_recipe.parquet"),
            pl.col("company_id") == company_id,
        ),
        (
            RecipeEmbedding.location,
            cache_dir.parquet_at(f"{company_id}/recipe_embeddings.parquet"),
            pl.col("company_id") == company_id,
        ),
    ]

    custom_cache = {loc for loc, _, _ in cache_sources}

    for dep in depends_on:
        if dep in custom_cache:
            continue

        cache_sources.append((dep, cache_dir.parquet_at(f"{dep.name}.parquet"), None))

    load_for = [(loc, source, pl_filter) for loc, source, pl_filter in cache_sources if loc not in exclude_views]

    return await load_cache_for(store, load_for, force_load=force_load)


def convert_concepts_to_attributes(
    request: GenerateMealkitRequest,
) -> GenerateMealkitRequest:
    if not request.concept_preference_ids:
        request.concept_preference_ids = ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"]
        return request

    mappings = {
        # GL
        # Ekspress GL
        "DEF1DD75-F6EA-40F9-9FA5-B6C0583797EE": ["C28F210B-427E-45FA-9150-D6344CAE669B"],
        # Flexitarianer GL
        "4A3E19DF-9524-4308-B927-BD20522628B0": [
            "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "6A494593-2931-4269-80EE-470D38F04796",
        ],
        # Vegetar
        "1B7DBFF1-302E-4219-8108-BB5AA5B95D06": ["6A494593-2931-4269-80EE-470D38F04796"],
        # Favorittkassen
        "44AE1E54-0BC8-4501-873D-BDCE85B7D9BC": ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"],
        # Single
        "A3523A3F-AB14-469F-ABA8-6F4938007A17": ["37CE056F-4779-4593-949A-42478734F747"],
        # Roede
        "047FB5EC-D64C-4F72-AB69-BF6171D559FC": ["DF81FF77-B4C4-4FC1-A135-AB7B0704D1FA"],
        # AMK
        # Anbefalt
        "7A326879-8A74-4AA8-8EC0-00EA3BB2AB32": [
            "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "B172864F-D58E-4395-B182-26C6A1F1C746",
        ],
        # Insp
        "673CD4EC-F637-4D0A-908D-1291D961A21F": ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"],
        # Sunn og lett
        "552E0438-2885-49F6-B990-677A8C46010E": ["FD661CAD-7F45-4D02-A36E-12720D5C16CA"],
        # Express
        "1DF054FB-2CBA-4AF2-AFDB-684CAE328CB2": ["C28F210B-427E-45FA-9150-D6344CAE669B"],
        # Glutenfri
        "E8629185-5EC2-47FC-8EA9-4ED2DCF6CCF8": ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"],
        # Laktosefri
        "2DEACFA4-FDC7-406A-BFC0-EBE472FCC9A0": ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"],
        # Vegetar
        "C5E32939-0650-4666-B386-772FA1A7BDBA": ["C28F210B-427E-45FA-9150-D6344CAE669B"],
        # RT:
        # Flexitari
        "1374B627-9D05-4BD9-A984-B6CEB84E55DB": [
            "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "6A494593-2931-4269-80EE-470D38F04796",
        ],
        # Veg
        "84809B25-13A1-4E86-935C-4347310A4D32": ["6A494593-2931-4269-80EE-470D38F04796"],
        # Favorit
        "E788CB65-286D-4C78-A0CD-434C2809C1FE": ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"],
        # Hurtig
        "6C1EA610-872D-4EB1-B821-38AB4BB9D457": ["C28F210B-427E-45FA-9150-D6344CAE669B"],
        # LMK
        # Favorit
        "594170F4-938D-4237-97A3-26FFAA261A29": ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"],
        # Inspir
        "94CD2E41-D1BF-49A6-BD1F-357B8754D86E": ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"],
        # Vegan
        "B722CE47-0F0A-4666-8A52-5C1EEBB80A8C": ["6A494593-2931-4269-80EE-470D38F04796"],
        # Vekt
        "CD1F8246-54C7-4058-911D-B52DC90FA011": ["FD661CAD-7F45-4D02-A36E-12720D5C16CA"],
        # Gluten
        "D76E0580-ACFE-4447-921F-B535705E1FB1": ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"],
        # Lactose
        "557CE16C-8920-4623-9F07-BD0BFDF2EAF9": ["C94BCC7E-C023-40CE-81E0-C34DA3D79545"],
        # Express
        "F4C8B40C-5C86-4A4C-9B20-E62B9AE83220": ["C28F210B-427E-45FA-9150-D6344CAE669B"],
        # Vegetarisk
        "306EEA6B-1FA5-46B4-8D30-FBEA4E9C3EE0": ["6A494593-2931-4269-80EE-470D38F04796"],
    }

    taste_preferences = {
        # GL
        # Flexi
        "4A3E19DF-9524-4308-B927-BD20522628B0": [
            "03D80B36-29DA-4B65-8220-75A32F419593",
            "1C936351-EB0F-4815-95F7-0C942CEA6CC3",
            "FF9F03BE-C0EB-4D79-BF40-0659AEDD3B89",
        ],
        # AMK
        # Lactoce
        "2DEACFA4-FDC7-406A-BFC0-EBE472FCC9A0": ["8A2D1C34-692A-4DE0-9224-4D7567C01D13"],
        # Gluten
        "E8629185-5EC2-47FC-8EA9-4ED2DCF6CCF8": ["254B6C88-85AC-4C46-8C8E-E54BDEDCA565"],
        # LMK
        # Vegan
        "B722CE47-0F0A-4666-8A52-5C1EEBB80A8C": [
            "03D80B36-29DA-4B65-8220-75A32F419593",
            "1C936351-EB0F-4815-95F7-0C942CEA6CC3",
            "FF9F03BE-C0EB-4D79-BF40-0659AEDD3B89",
            "9EEB4F91-6402-4247-A9E5-39BD8CDE5526",
            "95217648-0CA2-4E7B-A849-52D70EB7BC17",
            "82E6F0C1-0640-498E-8B15-6C8C5DCA5336",
            "9AD1019D-A43D-4640-9034-D8B7DEB02D0E",
            "54555183-10BA-40E2-9408-ADB878B0AC82",
        ],
        # Lactose
        "557CE16C-8920-4623-9F07-BD0BFDF2EAF9": ["8A2D1C34-692A-4DE0-9224-4D7567C01D13"],
        # Gluten
        "D76E0580-ACFE-4447-921F-B535705E1FB1": ["254B6C88-85AC-4C46-8C8E-E54BDEDCA565"],
        # RT
        # Flexi
        "1374B627-9D05-4BD9-A984-B6CEB84E55DB": [
            "03D80B36-29DA-4B65-8220-75A32F419593",
            "1C936351-EB0F-4815-95F7-0C942CEA6CC3",
            "FF9F03BE-C0EB-4D79-BF40-0659AEDD3B89",
        ],
    }

    all_taste_preference = set(request.taste_preferences)
    all_attribute_ids = set()
    potential_attribute_ids = set()

    for ids in mappings.values():
        potential_attribute_ids.update(ids)

    for concept_id in request.concept_preference_ids:
        all_taste_preference.update(
            [
                NegativePreference(preference_id=pref, is_allergy=True)
                for pref in taste_preferences.get(concept_id, set())
            ]
        )
        all_attribute_ids.update(mappings.get(concept_id, set()))

    if not all_attribute_ids:
        all_attribute_ids = {
            concept_id for concept_id in request.concept_preference_ids if concept_id in potential_attribute_ids
        }

    assert all_attribute_ids, f"Found no attribute ids for {request.concept_preference_ids}"

    request.taste_preferences = list(all_taste_preference)
    request.concept_preference_ids = list(all_attribute_ids)
    return request


async def process_stream_batch(
    store: ContractStore,
    stream: ReadableStream[GenerateMealkitRequest],
    successful_output_stream: WritableStream | None,
    failed_output_stream: WritableStream | None,
    progress_callback: Callable[[int, int], None] | None = None,
    progress_callback_interval: int = 5,
    write_batch_interval: int = 100,
) -> int:
    from datadog.dogstatsd.base import statsd

    with duration("read_batch"):
        messages = await stream.read()

    number_of_messages = len(messages)

    if number_of_messages == 0:
        return number_of_messages

    statsd.increment("preselector.processed-requests", number_of_messages)

    successful_responses: list[PreselectorSuccessfulResponse] = []
    completed_requests: list[StreamMessage] = []
    failed_requests: list[PreselectorFailedResponse] = []

    with duration("process_messages"):
        for index, message in enumerate(messages):
            raw_request = message.body
            statsd.increment("preselector.processed_weeks", len(message.body.compute_for))

            with duration("process_single_message"):
                try:
                    request = convert_concepts_to_attributes(raw_request.to_upper_case_ids())
                except Exception as e:
                    logger.exception(f"Error converting {raw_request}")
                    logger.exception(e)

                    failed_requests.append(
                        PreselectorFailedResponse(error_message=str(e), error_code=500, request=raw_request)
                    )
                    continue

                try:
                    result = await run_preselector_for_request(request, store)
                    success_response = result.success_response()
                    if success_response:
                        successful_responses.append(success_response)

                    failed_requests.extend(result.failed_responses())
                except Exception as e:
                    logger.exception(f"Error processing request {request}")
                    logger.exception(e)

                    failed_requests.append(
                        PreselectorFailedResponse(error_message=str(e), error_code=500, request=request)
                    )

            completed_requests.append(message)

            if progress_callback and index % progress_callback_interval == 0:
                progress_callback(index + 1, number_of_messages)

            if index % write_batch_interval == 0 and index != 0:
                with duration("write_batch"):
                    if successful_output_stream:
                        await successful_output_stream.batch_write(successful_responses)
                        successful_responses = []

                    if failed_output_stream and failed_requests:
                        failed_ids = [req.request.agreement_id for req in failed_requests]
                        logger.error(f"Error for user with id: {failed_ids}")
                        await failed_output_stream.batch_write(failed_requests)
                        failed_requests = []

                    await stream.mark_as_complete(completed_requests)
                    completed_requests = []

        with duration("write_batch"):
            if successful_output_stream:
                await successful_output_stream.batch_write(successful_responses)

            if failed_output_stream and failed_requests:
                failed_ids = [req.request.agreement_id for req in failed_requests]
                logger.error(f"Error for user with id: {failed_ids}")
                await failed_output_stream.batch_write(failed_requests)

            await stream.mark_as_complete(completed_requests)

        if progress_callback:
            progress_callback(number_of_messages, number_of_messages)

    return number_of_messages


async def create_service_bus_subscription(subscription_name: str, topic_name: str, namespace: str) -> None:
    from azure.identity import DefaultAzureCredential
    from azure.servicebus.management import ServiceBusAdministrationClient

    client = ServiceBusAdministrationClient(fully_qualified_namespace=namespace, credential=DefaultAzureCredential())
    try:
        subscription = client.get_subscription(topic_name, subscription_name=subscription_name)
        logger.info(f"Found '{subscription.name}' in '{topic_name}'. Will skip creation.")
    except ResourceNotFoundError:
        client.create_subscription(topic_name, subscription_name=subscription_name)


async def process_stream(
    settings: ProcessStreamSettings,
    min_sleep_time: float = 0.1,
) -> None:
    from cheffelo_logging import setup_datadog
    from datadog import initialize

    logging.basicConfig(level=logging.INFO)

    # Removing unwanted logging
    logging.getLogger("azure").setLevel(logging.ERROR)
    logging.getLogger("aligned").setLevel(logging.ERROR)

    config = DataDogConfig()  # type: ignore[reportGeneralTypeIssues]

    setup_datadog(logging.getLogger(""), config)

    try:

        def use_serverless_databricks(source: DatabricksSource, location: FeatureLocation) -> None:
            source.config = DatabricksConnectionConfig.serverless(
                host=settings.databricks_host, token=settings.databricks_token.get_secret_value()
            )

        original_store = preselector_store()
        original_store.sources_of_type(DatabricksSource, use_serverless_databricks)

        datadog_config = DataDogStatsdConfig()  # type: ignore[reportGeneralTypeIssues]
        initialize(
            statsd_host=datadog_config.datadog_host,
            statsd_port=datadog_config.datadog_port,
            statsd_constant_tags=config.datadog_tags.split(","),
        )
        logger.info(settings)

        company_map = {
            "godtlevert": "09ECD4F0-AE58-4539-8E8F-9275B1859A19",
            "adams": "8A613C15-35E4-471F-91CC-972F933331D7",
            "linas": "6A2D0B60-84D6-4830-9945-58D518D27AC2",
            "retnemt": "5E65A955-7B1A-446C-B24F-CFE576BF52D7",
        }
        assert settings.service_bus_subscription_name is not None
        company_id = company_map[settings.service_bus_subscription_name]
        streams = await connect_to_streams(settings, company_id)

    except Exception as error:
        logger.exception(error)
        logger.error("Exited worker before we could start processing.")
        return

    try:
        store = await load_cache(original_store, company_id)
        last_cache_load = datetime.now(tz=timezone.utc)
        last_batch_write = datetime.now(tz=timezone.utc)
    except Exception as error:
        logger.exception(error)
        raise error

    try:
        logger.info(f"'{settings.service_bus_subscription_name}' ready for some cooking!")
        while True:
            batch_start = time.monotonic()
            with duration("run_full_batch"):
                number_of_messages = await process_stream_batch(
                    store,
                    streams.request_stream,
                    streams.successful_output_stream,
                    streams.failed_output_stream,
                )
            batch_end = time.monotonic()

            batch_duration = batch_end - batch_start
            sleep_time = max(settings.ideal_poll_interval - batch_duration, min_sleep_time)
            logger.debug(f"Done a batch of work {batch_duration:.3f}, sleeping for {sleep_time:.3f} seconds")

            now = datetime.now(tz=timezone.utc)
            if number_of_messages == 0 and ((now - last_cache_load) > settings.update_data_interval):
                # Only update the cached data if there are no messages to process
                # And the cache is 3 hours old
                store = await load_cache(original_store, company_id, force_load=True)
                last_cache_load = datetime.now(tz=timezone.utc)

            if (
                number_of_messages == 0
                and settings.write_output_interval
                and (now - last_batch_write > settings.write_output_interval)
                and streams.successful_output_stream
                and streams.failed_output_stream
            ):
                await write_to_databricks(
                    streams.successful_output_stream.reader(PreselectorSuccessfulResponse),
                    streams.failed_output_stream.reader(PreselectorFailedResponse),
                    original_store,
                    write_size=settings.write_output_max_size,
                    max_wait_time=settings.write_output_wait_time,
                )
                last_batch_write = datetime.now(tz=timezone.utc)

            await asyncio.sleep(sleep_time)
    except (SessionLockLostError, MessageAlreadySettled) as error:
        logger.error("Error when settling bus message")
        logger.exception(error)
    except Exception as error:
        logger.error("Error while processing pre-selector requests")
        logger.exception(error)
        raise error


if __name__ == "__main__":
    asyncio.run(
        process_stream(
            settings=ProcessStreamSettings()  # type: ignore[reportGeneralTypeIssues]
        )
    )
