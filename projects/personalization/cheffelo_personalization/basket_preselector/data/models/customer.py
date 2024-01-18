from typing import List, Optional

from pydantic import BaseModel

from .order import DeliveredOrder, PlannedOrder
from .product import ProductVariation
from .utils import Yearweek

CONCEPT_PREFERENCE_TYPE_ID = "009CF63E-6E84-446C-9CE4-AFDBB6BB9687"
TASTE_PREFERENCE_TYPE_ID = "4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB"


class Preference(BaseModel):
    preference_priority: Optional[int] = 0
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
    delivered_orders: Optional[List[DeliveredOrder]]
    planned_orders: Optional[List[PlannedOrder]]
    subscribed_product_variation_id: str


class CustomerBatch(BaseModel):
    company_id: str
    week: list[Yearweek]
    agreements: list[Customer]
