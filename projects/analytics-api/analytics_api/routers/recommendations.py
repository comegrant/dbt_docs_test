import logging
from typing import Annotated

from aligned import ContractStore
from constants.companies import CompanyID
from data_contracts.helper import snake_to_camel
from data_contracts.reci_pick import DefaultRecommendations, LatestRecommendations
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from analytics_api.settings import EnvSettings, load_settings
from analytics_api.store import load_store
from analytics_api.utils.auth import TokenContent, extract_token_content

logger = logging.getLogger(__name__)


class RecommendationsParams(BaseModel):
    menu_year: int = Field(alias="year")
    menu_week: int = Field(alias="week")

    company_id: CompanyID | None = None
    billing_agreement_id: int | None = Field(None)


class RecipeRow(BaseModel):
    main_recipe_id: int
    score: float

    class Config:
        alias_generator = snake_to_camel


class RecommendationResponse(BaseModel):
    billing_agreement_id: int
    company_id: str
    recipes: list[RecipeRow] | None
    model_version: str | None

    menu_week: int = Field(alias="week")
    menu_year: int = Field(alias="year")

    class Config:
        alias_generator = snake_to_camel


rec_router = APIRouter(tags=["Recommendations"])


@rec_router.get("/recommendations")
async def read_recs(
    store: Annotated[ContractStore, Depends(load_store)],
    token: Annotated[TokenContent, Depends(extract_token_content)],
    settings: Annotated[EnvSettings, Depends(load_settings)],
    args: Annotated[RecommendationsParams, Depends()],
) -> RecommendationResponse:
    if settings.environment == "prod":
        # Do not use the user passed agreement id args in production
        # This can leak personal information, but it is useful for testing
        args.billing_agreement_id = token.billing_agreement_id
        args.company_id = token.company_id  # type: ignore
    else:
        args.billing_agreement_id = args.billing_agreement_id or token.billing_agreement_id
        args.company_id = args.company_id or token.company_id  # type: ignore

    entity_dict = args.model_dump(by_alias=False)
    user_recs = await store.feature_view(LatestRecommendations).features_for(entity_dict).to_polars()

    if user_recs["recipes"].to_list()[0] is not None:
        return RecommendationResponse.model_validate(user_recs.to_dicts()[0], by_name=True)

    default_recs = await store.feature_view(DefaultRecommendations).features_for(entity_dict).to_polars()
    return RecommendationResponse.model_validate(default_recs.to_dicts()[0], by_name=True)
