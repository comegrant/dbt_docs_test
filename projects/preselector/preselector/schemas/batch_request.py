from datetime import datetime
from uuid import uuid4

from pydantic import BaseModel, Field


class YearWeek(BaseModel):
    week: int
    year: int

class NegativePreference(BaseModel):
    preference_id: str
    is_allergy: bool

    def __hash__(self) -> int:
        return hash(self.preference_id)

class GenerateMealkitRequest(BaseModel):
    agreement_id: int
    company_id: str
    compute_for: list[YearWeek]

    concept_preference_ids: list[str]
    """Represents the different attributs that a user can select"""

    taste_preferences: list[NegativePreference]

    portion_size: int
    number_of_recipes: int
    override_deviation: bool

    originated_at: datetime | None = Field(default=None)
    ordered_weeks_ago: dict[int, int] | None = Field(default=None)
    has_data_processing_consent: bool = Field(False)
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))

    @property
    def taste_preference_ids(self) -> list[str]:
        return [
            taste.preference_id for taste in self.taste_preferences
        ]

    def to_upper_case_ids(self) -> 'GenerateMealkitRequest':
        self.company_id = self.company_id.upper()
        self.concept_preference_ids = [
            concept_id.upper() for concept_id in self.concept_preference_ids
        ]
        self.taste_preferences = [
            NegativePreference(
                preference_id=preference.preference_id.upper(),
                is_allergy=preference.is_allergy
            )
            for preference in self.taste_preferences
        ]
        return self
