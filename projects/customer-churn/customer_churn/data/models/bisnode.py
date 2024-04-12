from datetime import datetime

from pydantic import BaseModel


class Bisnode(BaseModel):
    agreement_id: int
    gedi: str
    impulsiveness: float
    installment_affinity: float
    profile_online_media: float
    cultural_class: str
    perceived_purchasing_power: str
    consumption: float
    profile_newspaper: float
    financial_class: str
    probability_children_0_to_6: float
    probability_children_7_to_17: float
    education_years: float
    purchase_power: float
    level_of_education: str
    life_stage: str
    type_of_housing: str
    confidence_level: str
    number_of_hits: int
    created_at: datetime
    updated_at: datetime
    new_car_buyer: str | None = None
    persons_in_household: float | None = None
    housing_cooperative: str | None = None
    household_id: float | None = None
