from preselector.data.models.customer import PreselectorYearWeekResponse
from preselector.schemas.batch_request import YearWeek

mealkit_cache = {}


def cached_output(
    concepts: list[str],
    year_week: YearWeek,
    portion_size: int,
    number_of_recipes: int,
    taste_preference_ids: list[str],
    company_id: str,
    ordered_in_week: dict[int, int] | None,
) -> PreselectorYearWeekResponse | None:
    key = f"{year_week.year}-{year_week.week}-{concepts}-{portion_size}-{number_of_recipes}-{taste_preference_ids}-{company_id}-{ordered_in_week}"  # noqa: E501
    return mealkit_cache.get(key)


def set_cache(
    result: PreselectorYearWeekResponse,
    concepts: list[str],
    year_week: YearWeek,
    portion_size: int,
    number_of_recipes: int,
    taste_preference_ids: list[str],
    company_id: str,
    ordered_in_week: dict[int, int] | None,
) -> None:
    key = f"{year_week.year}-{year_week.week}-{concepts}-{portion_size}-{number_of_recipes}-{taste_preference_ids}-{company_id}-{ordered_in_week}"  # noqa: E501
    mealkit_cache[key] = result
