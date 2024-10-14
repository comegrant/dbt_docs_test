import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from datetime import date
from typing import Annotated

import polars as pl
from aligned import ContractStore, FeatureLocation
from aligned.data_source.batch_data_source import BatchDataSource
from aligned.sources.in_mem_source import InMemorySource
from cheffelo_logging import setup_datadog
from cheffelo_logging.logging import DataDogConfig
from data_contracts.preselector.store import Preselector as PreselectorOutput
from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

from preselector.main import GenerateMealkitRequest, duration, run_preselector_for_request
from preselector.process_stream import load_cache_for
from preselector.schemas.batch_request import NegativePreference, YearWeek
from preselector.store import preselector_store

logger = logging.getLogger(__name__)


class GeneratePreview(BaseModel):

    taste_preferences: list[NegativePreference] | None
    attribute_ids: list[str]

    year: int
    week: int
    company_id: str

    portion_size: int

    def request(self) -> GenerateMealkitRequest:
        return GenerateMealkitRequest(
            agreement_id=0,
            company_id=self.company_id,
            compute_for=[YearWeek(week=self.week, year=self.year)],
            concept_preference_ids=self.attribute_ids,
            taste_preferences=self.taste_preferences or [],
            portion_size=self.portion_size,
            number_of_recipes=5,
            override_deviation=False,
            has_data_processing_consent=False,
            quarentine_main_recipe_ids=[]
        )

class Mealkit(BaseModel):
    main_recipe_ids: list[int]
    model_version: str


store: ContractStore | None = None

async def load_store() -> ContractStore:
    global store # noqa: PLW0603
    if store is not None:
        return store

    store = preselector_store()

    today = date.today()
    this_week = today.isocalendar().week

    cache_sources: list[tuple[FeatureLocation, BatchDataSource, pl.Expr | None]] = [
        (
            FeatureLocation.feature_view("recipe_cost"),
            InMemorySource.empty(),
            (pl.col("menu_year") >= today.year) & (pl.col("menu_week") > this_week),
        ),
        (
            FeatureLocation.feature_view("preselector_year_week_menu"),
            InMemorySource.empty(),
            (pl.col("menu_year") >= today.year)
        ),
        (
            FeatureLocation.feature_view("normalized_recipe_features"),
            InMemorySource.empty(),
            (pl.col("year") >= today.year)
        ),
    ]

    custom_cache = {loc for loc, _, _ in cache_sources}

    depends_on = store.feature_view(PreselectorOutput).view.source.depends_on()

    for dep in depends_on:
        if dep in custom_cache:
            continue

        if dep.location_type == "feature_view":
            entity_names = store.feature_view(dep.name).view.entitiy_names
            if "agreement_id" in entity_names:
                # Do not want any user spesific data
                logger.info(dep.name)
                store.feature_source.sources.pop(dep.identifier, None)
                continue

        cache_sources.append((dep, InMemorySource.empty(), None))


    logger.info(f"Loading data for {len(cache_sources)}")
    store = await load_cache_for(store, cache_sources)
    logger.info("Cache is hot")
    return store


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    import os

    logging.basicConfig(level=logging.INFO)

    logging.getLogger("azure").setLevel(logging.ERROR)
    logging.getLogger("aligned").setLevel(logging.ERROR)

    with suppress(ValidationError):
        # Adding data dog to the root
        datadog_tags = {
            "env": os.getenv('ENV'),
        }

        if "GIT_COMMIT_HASH" in os.environ:
            datadog_tags["git_sha"] = os.getenv("GIT_COMMIT_HASH")

        datadog_tag_string = ""
        for key, value in datadog_tags.items():
            datadog_tag_string += f"{key}:{value},"

        setup_datadog(
            logging.getLogger(""),
            config=DataDogConfig(
                datadog_service_name="pre-selector-api",
                datadog_source="python",
                datadog_tags=datadog_tag_string.strip(","),
            ) # type: ignore[reportGeneralTypeIssues]
        )

    await load_store()

    yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
def index() -> str:
    return "What's cooking?"


@app.post("/pre-selector/preview")
async def generate_preview(
    body: GeneratePreview,
    store: Annotated[ContractStore, Depends(load_store)]
) -> JSONResponse:
    """
    An endpoint used to preview the output of the pre-selector.
    """
    req = body.request()
    with duration("Generate", should_log=True):
        response = await run_preselector_for_request(
            req,
            store
        )

    success = response.success_response()
    if success:
        response = Mealkit(
            main_recipe_ids=success.year_weeks[0].main_recipe_ids,
            model_version=response.model_version
        ).model_dump()
    elif response.failures:
        logger.error(f"Failed for request {body}")
        response = response.failures[0].model_dump()
    else:
        raise ValueError("Found no successful or failure response. This should never happen.")

    cache_in_seconds = 60 * 5

    return JSONResponse(
        response,
        headers={
            "Cache-Control": f"max-age={cache_in_seconds}"
        }
    )
