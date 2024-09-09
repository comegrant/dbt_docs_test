from pydantic import BaseModel, Field


class YearWeek(BaseModel):
    week: int
    year: int


class GenerateMealkitRequest(BaseModel):
    agreement_id: int
    company_id: str
    compute_for: list[YearWeek]

    concept_preference_ids: list[str]
    """Represents the different attributs that a user can select"""

    taste_preference_ids: list[str]

    portion_size: int
    number_of_recipes: int
    override_deviation: bool

    quarentine_main_recipe_ids: list[int] = Field(default_factory=list)
    has_data_processing_consent: bool = Field(False)

    def to_upper_case_ids(self) -> 'GenerateMealkitRequest':
        self.company_id = self.company_id.upper()
        self.concept_preference_ids = [
            concept_id.upper() for concept_id in self.concept_preference_ids
        ]
        self.taste_preference_ids = [
            taste_id.upper() for taste_id in self.taste_preference_ids
        ]
        return self
