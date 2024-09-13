from uuid import uuid4

from pydantic import BaseModel, Field


class YearWeek(BaseModel):
    week: int
    year: int

class NegativePreference(BaseModel):
    preference_id: str
    is_allergy: bool

class GenerateMealkitRequest(BaseModel):
    agreement_id: int
    company_id: str
    compute_for: list[YearWeek]


    concept_preference_ids: list[str]
    """Represents the different attributs that a user can select"""

    taste_preferences: list[NegativePreference]

    @property
    def taste_preference_ids(self) -> list[str]:
        return [
            taste.preference_id for taste in self.taste_preferences
        ]

    portion_size: int
    number_of_recipes: int
    override_deviation: bool

    quarentine_main_recipe_ids: list[int] = Field(default_factory=list)
    has_data_processing_consent: bool = Field(False)
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))

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
