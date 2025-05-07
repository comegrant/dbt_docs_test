import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from typing import Annotated

from aligned import ContractStore
from cheffelo_logging import setup_datadog
from cheffelo_logging.logging import DataDogConfig
from fastapi import Depends, FastAPI
from pydantic import BaseModel, Field, ValidationError

from preselector.main import GenerateMealkitRequest, duration, run_preselector_for_request
from preselector.preview_server_store import load_store
from preselector.schemas.batch_request import NegativePreference, YearWeek

logger = logging.getLogger(__name__)


class GeneratePreview(BaseModel):
    taste_preferences: list[NegativePreference] | None
    attribute_ids: list[str]

    year: Annotated[int, Field(gt=2023)]
    week: Annotated[int, Field(gt=0, lt=54)]
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
        )


class Mealkit(BaseModel):
    main_recipe_ids: list[int]
    model_version: str


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    import os

    logging.basicConfig(level=logging.INFO)

    logging.getLogger("azure").setLevel(logging.ERROR)
    logging.getLogger("aligned").setLevel(logging.ERROR)

    with suppress(ValidationError):
        # Adding data dog to the root
        datadog_tags = {
            "env": os.getenv("ENV"),
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
            ),  # type: ignore[reportGeneralTypeIssues]
        )

    await load_store()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
def index() -> str:
    return "What's cooking?"


@app.post("/pre-selector/preview")
async def generate_preview(body: GeneratePreview, store: Annotated[ContractStore, Depends(load_store)]) -> Mealkit:
    """
    An endpoint used to preview the output of the pre-selector.
    """
    req = body.request()
    with duration("preview-api-generate"):
        try:
            response = await run_preselector_for_request(req, store)
        except Exception as error:
            logger.exception(error)
            raise error

    success = response.success_response()
    if success:
        response = Mealkit(main_recipe_ids=success.year_weeks[0].main_recipe_ids, model_version=response.model_version)
    elif response.failures:
        logger.error(f"Failed for request {body}")
        response = response.failures[0]
        raise ValueError(response.error_message)
    else:
        raise ValueError("Found no successful or failure response. This should never happen.")

    return response
