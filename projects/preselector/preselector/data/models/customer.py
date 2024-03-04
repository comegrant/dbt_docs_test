from pydantic import BaseModel, Field

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


class PreselectorResult(BaseModel):
    agreement_id: int
    main_recipe_ids: list[int]
    debug_summary: dict


class RunConfig(BaseModel):
    should_filter_portion_size: bool = Field(default=True)
    should_filter_taste_restrictions: bool = Field(default=True)

    should_rank_using_rec_engine: bool = Field(default=True)
    should_rank_with_quarantine: bool = Field(default=True)

    should_evaluate_preference_rules: bool = Field(default=True)
    should_evaluate_protein_variety: bool = Field(default=True)
    should_evaluate_cost_of_food: bool = Field(default=True)
