from datetime import datetime
from enum import IntEnum

from pydantic import BaseModel, Field

from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference

from .order import DeliveredOrder, PlannedOrder
from .product import ProductVariation
from .utils import Yearweek

CONCEPT_PREFERENCE_TYPE_ID = "009CF63E-6E84-446C-9CE4-AFDBB6BB9687"
TASTE_PREFERENCE_TYPE_ID = "4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB"


class Preference(BaseModel):
    preference_priority: int | None = 0
    preference_id: str
    preference_type_id: str
    preference_name: str


class Customer(BaseModel):
    agreement_id: int
    company_id: str
    status: int
    concept_preference_id: str
    basket_variation: ProductVariation
    variation_portions: int
    delivered_orders: list[DeliveredOrder] | None
    planned_orders: list[PlannedOrder] | None
    subscribed_product_variation_id: str


class CustomerBatch(BaseModel):
    company_id: str
    week: list[Yearweek]
    agreements: list[Customer]


class PreselectorTestParameters(BaseModel):
    agreement_id: int


class PreselectorCustomer(BaseModel):
    agreement_id: int
    company_id: str
    concept_preference_id: str
    taste_preference_ids: list[str] | None

    portion_size: int
    number_of_recipes: int

    subscribed_product_variation_id: str | None = Field(default=None)


class PreselectorPreferenceCompliancy(IntEnum):
    non_preference_complient = 1
    allergies_only_complient = 2
    all_complient = 3


class PreselectorYearWeekResponse(BaseModel):
    year: int
    week: int
    portion_size: int
    variation_ids: list[str]
    main_recipe_ids: list[int]
    compliancy: PreselectorPreferenceCompliancy
    target_cost_of_food_per_recipe: float
    quarantined_recipe_ids: list[int] | None = Field(None)
    generated_recipe_ids: dict[int, int] | None = Field(None)


class PreselectorSuccessfulResponse(BaseModel):
    agreement_id: int

    correlation_id: str
    year_weeks: list[PreselectorYearWeekResponse]

    concept_preference_ids: list[str]
    taste_preferences: list[NegativePreference]

    override_deviation: bool
    "Echoing the value from the request. Is useful for CMS."

    model_version: str | None
    generated_at: datetime

    version: int = Field(default=1)
    "The schema version"

class PreselectorFailedResponse(BaseModel):
    error_message: str
    error_code: int
    request: GenerateMealkitRequest


class PreselectorFailure(BaseModel):
    error_message: str
    error_code: int
    year: int
    week: int


class RunConfig(BaseModel):
    should_filter_portion_size: bool = Field(default=True)
    should_filter_taste_restrictions: bool = Field(default=True)

    should_rank_using_rec_engine: bool = Field(default=True)
    should_rank_with_quarantine: bool = Field(default=True)

    should_evaluate_preference_rules: bool = Field(default=True)
    should_evaluate_protein_variety: bool = Field(default=True)
    should_evaluate_cost_of_food: bool = Field(default=True)
