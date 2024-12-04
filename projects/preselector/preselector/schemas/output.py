from dataclasses import dataclass

from preselector.data.models.customer import PreselectorPreferenceCompliancy


@dataclass
class PreselectorWeekOutput:
    main_recipe_ids: list[int]
    compliancy: PreselectorPreferenceCompliancy
    error_vector: dict[str, float]
