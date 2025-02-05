from dataclasses import dataclass

from preselector.data.models.customer import PreselectorPreferenceCompliancy


@dataclass
class PreselectorRecipe:
    main_recipe_id: int
    compliancy: PreselectorPreferenceCompliancy

@dataclass
class PreselectorWeekOutput:
    recipes: list[PreselectorRecipe]
    error_vector: dict[str, float] | None

    @property
    def main_recipe_ids(self) -> list[int]:
        return [
            rec.main_recipe_id for rec in self.recipes
        ]
