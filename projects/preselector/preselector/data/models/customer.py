import json
from datetime import datetime
from enum import IntEnum
from typing import Annotated

import polars as pl
from aligned.schemas.feature import StaticFeatureTags
from data_contracts.preselector.basket_features import BasketFeatures
from data_contracts.preselector.store import FailedPreselectorOutput, Preselector, SuccessfulPreselectorOutput
from pydantic import BaseModel, Field, computed_field

from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference


class PreselectorCustomer(BaseModel):
    agreement_id: int
    company_id: str
    concept_preference_id: str
    taste_preference_ids: list[str] | None

    portion_size: int
    number_of_recipes: int

    subscribed_product_variation_id: str | None = Field(default=None)


class PreselectorPreferenceCompliancy(IntEnum):
    non_preference_compliant = 1
    allergies_only_compliant = 2
    all_compliant = 3


class PreselectorRecipeResponse(BaseModel):
    main_recipe_id: int
    variation_id: str
    compliancy: PreselectorPreferenceCompliancy

class PreselectorYearWeekResponse(BaseModel):
    year: int
    week: int
    target_cost_of_food_per_recipe: float
    recipes_data: list[PreselectorRecipeResponse]
    ordered_weeks_ago: Annotated[dict[int, int] | None, Field] = None
    error_vector: Annotated[dict[str, float | None] | None, Field] = None

    @computed_field
    @property
    def variation_ids(self) -> list[str]:
        "Is here to keep backwards compatability"
        return [
            rec.variation_id for rec in self.recipes_data
        ]

    @computed_field
    @property
    def main_recipe_ids(self) -> list[int]:
        "Is here to keep backwards compatability"
        return [
            rec.main_recipe_id for rec in self.recipes_data
        ]

    @computed_field
    @property
    def compliancy(self) -> PreselectorPreferenceCompliancy:
        "Is here to keep backwards compatability"
        return PreselectorPreferenceCompliancy(min(
            rec.compliancy.real for rec in self.recipes_data
        ))


class PreselectorSuccessfulResponse(BaseModel):
    agreement_id: int
    company_id: str

    correlation_id: str
    year_weeks: list[PreselectorYearWeekResponse]

    concept_preference_ids: list[str]
    taste_preferences: list[NegativePreference]

    override_deviation: bool
    "Echoing the value from the request. Is useful for CMS."

    model_version: str
    generated_at: datetime

    has_data_processing_consent: bool

    number_of_recipes: int | None = Field(default=None)
    portion_size: int | None = Field(default=None)
    originated_at: datetime | None = Field(default=None)

    version: int = Field(default=1)
    "The schema version"

    def to_dataframe(self) -> Annotated[pl.DataFrame, SuccessfulPreselectorOutput]:
        """
        Returns a dataframe that conforms to the data contract `SuccessfulPreselectorOutput`.
        """
        request_values = self.model_dump(exclude={"year_weeks", "originated_at"})
        request_values["taste_preferences"] = json.dumps([ pref.model_dump() for pref in self.taste_preferences ])
        request_values["taste_preference_ids"] = [ pref.preference_id for pref in self.taste_preferences ]
        generated_weeks = []

        error_features = [
            feat.name for feat
            in BasketFeatures.query().request.all_returned_features
            if StaticFeatureTags.is_entity not in (feat.tags or [])
        ]
        returned_features = SuccessfulPreselectorOutput.query().request.all_returned_features
        returned_features_in_batch = Preselector.query().request.all_returned_columns
        all_returned_feature_names = {
            feat.name for feat in returned_features
        }.union(returned_features_in_batch)

        expected_schema = {
            feat.name: feat.dtype.polars_type
            for feat in returned_features
            if "json" not in feat.dtype.name
        }
        error_vector_type = pl.Struct({
            feat: pl.Float64
            for feat in error_features
        })
        expected_schema["error_vector"] = error_vector_type
        renames = {
            "agreement_id": "billing_agreement_id",
            "week": "menu_week",
            "year": "menu_year",
        }

        for mealkit in self.year_weeks:
            mealkit_dict = mealkit.model_dump()
            mealkit_dict.update(request_values)

            # Need to convert the keys to strings,
            # as polars do not support ints as the key type
            mealkit_dict["ordered_weeks_ago"] = json.dumps({
                str(key): value
                for key, value in mealkit.ordered_weeks_ago.items()
            }) if mealkit.ordered_weeks_ago else None

            mealkit_dict["recipes"] = [
                recipe.model_dump()
                for recipe in mealkit.recipes_data
            ]

            for old_key, new_key in renames.items():
                mealkit_dict[new_key] = mealkit_dict[old_key]

            if mealkit.error_vector:
                mealkit_dict["error_vector"] = {
                    key: value
                    for key, value
                    in mealkit.error_vector.items()
                    if key in error_features
                }
            else:
                mealkit_dict["error_vector"] = None

            generated_weeks.append({
                key: value for key, value in mealkit_dict.items()
                if key in all_returned_feature_names
            })

        return pl.DataFrame(
            generated_weeks,
            schema_overrides=expected_schema
        )

class PreselectorFailedResponse(BaseModel):
    error_message: str
    error_code: int
    request: GenerateMealkitRequest


    def to_dataframe(self) -> Annotated[pl.DataFrame, FailedPreselectorOutput]:
        dict_values = self.model_dump(exclude={"request"})

        # Want this request as a raw json string
        # Makes it easier to handle schema changes etc.
        dict_values["request"] = self.request.model_dump_json()

        return pl.DataFrame([dict_values])


class PreselectorFailure(BaseModel):
    error_message: str
    error_code: int
    year: int
    week: int
