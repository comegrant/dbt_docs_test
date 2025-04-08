import math
from collections import Counter, defaultdict

import polars as pl
from catalog_connector import connection
from pydantic import BaseModel, Field

AVAILABLE_PREFS = {
    "Gluten",
    "Lactose",
    "Pork",
    "Poultry",
    "Beef",
    "Lamb",
    "Shellfish",
    "Fish",
    "Egg",
    "Dairy",
    "Non-vegetarian",
    "Nuts",
}
AVAILABLE_PROTEINS = {
    "Pork",
    "Poultry",
    "Beef",
    "Lamb",
    "Shellfish",
    "Fish",
    "Non-vegetarian",
}
VEGETARIAN_MAIN_INGREDIENT_ID = 3
PREVIOUS_WEEKS = 3
MAX_PORTION_SIZE = 5
ENTROPY_THRESHOLD = 0.75

WARNING_ID_MAP = {
    "warning_critical": 1,
    "warning_protein_entropy": 2,
    "warning_overlap": 3,
}


class PortionModel(BaseModel):
    portion_id: int = Field(alias="PortionId")
    main_recipe_ids: list[int] = Field(alias="MainRecipeIds")


class MenuWeekModel(BaseModel):
    menu_week: int = Field(alias="MenuWeek")
    menu_year: int = Field(alias="MenuYear")
    company_id: str = Field(alias="CompanyId")
    portions: list[PortionModel] = Field(alias="Portions")


class MenuFeedbackRequestModel(BaseModel):
    target_menu_week: MenuWeekModel = Field(alias="TargetMenuWeek")
    previous_menu_weeks: list[MenuWeekModel] = Field(alias="PreviousMenuWeeks")


class PreferenceGroupModel(BaseModel):
    preference_name: str = Field(alias="PreferenceName")
    user_count: int = Field(alias="UserCount")
    warning_type: list[int] = Field(alias="WarningType")


class PortionFeedbackModel(BaseModel):
    portion_id: int = Field(alias="PortionId")
    preference_groups: list[PreferenceGroupModel] = Field(alias="PreferenceGroups")


class MenuWeekFeedbackModel(BaseModel):
    menu_week: int = Field(alias="MenuWeek")
    menu_year: int = Field(alias="MenuYear")
    company_id: str = Field(alias="CompanyId")
    portions: list[PortionFeedbackModel] = Field(alias="Portions")


class MenuFeedbackResponseModel(BaseModel):
    result_menu_week: MenuWeekFeedbackModel = Field(alias="TargetMenuWeek")
    status: str = "success"


def get_user_preferences(company_id: str) -> pl.DataFrame:
    """Get user preferences for a company."""

    df = connection.sql(
        f"""
        select
        *
        from mlgold.menu_feedback_agreement_preferences_aggregated
        where company_id = '{company_id}'
        """
    ).toPandas()

    return pl.from_pandas(df)


def get_recipe_preferences(main_recipe_ids: list[int]) -> pl.DataFrame:
    """Get preferences for a list of main recipe ids."""

    if not main_recipe_ids:
        raise ValueError("main_recipe_ids must be a list of integers")

    main_recipe_ids_str = ", ".join(str(main_recipe_id) for main_recipe_id in main_recipe_ids)
    df = connection.sql(
        f"""
        select
        *
        from mlgold.menu_feedback_recipe_preferences
        where main_recipe_id in ({main_recipe_ids_str})
    """
    ).toPandas()

    return pl.from_pandas(df)


def get_previous_week_recipe_ids(payload: MenuFeedbackRequestModel, portion_id: int) -> dict[int, list[int]]:
    """Get the previous weeks recipe ids for a portion id."""
    recipe_ids_dict = {}
    for week_data in payload.previous_menu_weeks:
        week = week_data.menu_week
        recipe_ids_dict[week] = next(p.main_recipe_ids for p in week_data.portions if p.portion_id == portion_id)
    return recipe_ids_dict


def create_preference_set(value: str | None) -> list[str]:
    """Create a set of preferences from a string containing the preferences."""
    if value is None or value.strip() == "":
        return []
    return list({item.strip() for item in value.split(",")})


def find_matching_values(
    user_preferences: set[str],
    values: list,
    recipe_preferences: list[set[str]],
    recipe_main_ingredient_ids: list[int],
) -> list[int]:
    return [
        values[i]
        for i, recipe_preference in enumerate(recipe_preferences)
        if user_preferences.isdisjoint(recipe_preference)
        and not (
            "Non-vegetarian" in user_preferences and recipe_main_ingredient_ids[i] == VEGETARIAN_MAIN_INGREDIENT_ID
        )
    ]


def shannon_entropy(protein_counts: Counter) -> float:
    total = sum(protein_counts.values())
    return -sum((count / total) * math.log2(count / total) for count in protein_counts.values())


def entropy_score(protein_counts: Counter, total_max: int) -> float:
    if not protein_counts:
        return 0
    actual_entropy = shannon_entropy(protein_counts)
    max_entropy = math.log2(total_max) if protein_counts else 0
    return actual_entropy / max_entropy if max_entropy else 1


def check_overlap(row: dict, old_recipes: dict[int, list[int]], max_var: int, target_week: int) -> bool:
    if max_var == 0:
        return True
    elif max_var > PREVIOUS_WEEKS:
        return False

    overlap_set = set(row["match_recipes"])

    for i in range(1, max_var + 1):
        previous_week = target_week - i
        week_recipes = old_recipes.get(previous_week, [])
        overlap_set = overlap_set & set(week_recipes)
        if not overlap_set:
            return False

    return True


def response_from_stats(df: pl.DataFrame, year: int, week: int, company_id: str) -> MenuFeedbackResponseModel:
    grouped = defaultdict(list)
    for row in df.iter_rows(named=True):
        portion_id = row["portion_id"]
        preference_name = row["negative_taste_preferences"]
        user_count = row["number_of_users"]

        warning_type = [warning_id for col, warning_id in WARNING_ID_MAP.items() if row[col]]

        grouped[portion_id].append(
            PreferenceGroupModel(PreferenceName=preference_name, UserCount=user_count, WarningType=warning_type)
        )

    portions = [
        PortionFeedbackModel(PortionId=portion_id, PreferenceGroups=preferences)
        for portion_id, preferences in grouped.items()
    ]

    result_menu_week = MenuWeekFeedbackModel(MenuWeek=week, MenuYear=year, CompanyId=company_id, Portions=portions)

    return MenuFeedbackResponseModel(TargetMenuWeek=result_menu_week)


def create_warnings(
    payload: MenuFeedbackRequestModel,
    target_week: int,
    company_id: str,
    portion_id: int,
) -> pl.DataFrame:
    customers = get_user_preferences(company_id)
    recipes = get_recipe_preferences(
        next(p.main_recipe_ids for p in payload.target_menu_week.portions if p.portion_id == portion_id),
    )
    old_recipes = get_previous_week_recipe_ids(payload, portion_id)

    recipes = recipes.with_columns(
        pl.col("negative_taste_preferences")
        .map_elements(create_preference_set, return_dtype=pl.List(pl.Utf8))
        .alias("preference_set")
    )

    recipe_ids = recipes["main_recipe_id"].to_list()
    recipe_preferences = [set(x) for x in recipes["preference_set"].to_list()]
    recipe_main_ingredient_ids = recipes["recipe_main_ingredient_id"].to_list()

    customers = customers.with_columns(pl.lit(portion_id).alias("portion_id"))

    customers = customers.with_columns(
        pl.col("negative_taste_preferences")
        .map_elements(create_preference_set, return_dtype=pl.List(pl.Utf8))
        .alias("preference_set")
    )

    customers = customers.with_columns(
        [
            pl.col("preference_set")
            .map_elements(
                lambda x: find_matching_values(
                    user_preferences=set(x),
                    values=recipe_ids,
                    recipe_preferences=recipe_preferences,
                    recipe_main_ingredient_ids=recipe_main_ingredient_ids,
                ),
                return_dtype=pl.List(pl.Int64),
            )
            .alias("match_recipes"),
            pl.col("preference_set")
            .map_elements(
                lambda x: find_matching_values(
                    user_preferences=set(x),
                    values=recipe_main_ingredient_ids,
                    recipe_preferences=recipe_preferences,
                    recipe_main_ingredient_ids=recipe_main_ingredient_ids,
                ),
                return_dtype=pl.List(pl.Int64),
            )
            .alias("match_ingredient_ids"),
        ]
    )

    customers = customers.with_columns(
        pl.col("match_recipes").map_elements(len, return_dtype=pl.Int64).alias("n_recipes")
    )

    customers = customers.with_columns(
        [
            pl.col("preference_set")
            .map_elements(lambda x: list(AVAILABLE_PREFS - set(x)), return_dtype=pl.List(pl.Utf8))
            .alias("avail_prefs"),
            pl.col("preference_set")
            .map_elements(lambda x: list(AVAILABLE_PROTEINS - set(x)), return_dtype=pl.List(pl.Utf8))
            .alias("avail_proteins"),
        ]
    )

    customers = customers.with_columns(
        [
            pl.col("avail_prefs")
            .map_elements(
                lambda x: list((set(x) - {"Non-vegetarian"}) | {"Vegetarian"}) if "Non-vegetarian" in x else x,
                return_dtype=pl.List(pl.Utf8),
            )
            .alias("avail_prefs"),
            pl.col("avail_proteins")
            .map_elements(
                lambda x: list((set(x) - {"Non-vegetarian"}) | {"Vegetarian"}) if "Non-vegetarian" in x else x,
                return_dtype=pl.List(pl.Utf8),
            )
            .alias("avail_proteins"),
        ]
    )

    customers = customers.with_columns(
        pl.col("avail_proteins").map_elements(len, return_dtype=pl.Int64).alias("max_protein_var")
    )

    customers = customers.with_columns(
        pl.struct(["match_ingredient_ids", "max_protein_var"])
        .map_elements(
            lambda row: entropy_score(Counter(row["match_ingredient_ids"]), row["max_protein_var"]),
            return_dtype=pl.Float64,
        )
        .alias("protein_entropy_score")
    )

    customers = customers.with_columns(
        [
            (pl.col("n_recipes") // MAX_PORTION_SIZE).alias("max_week_var"),
            (pl.col("n_recipes") < MAX_PORTION_SIZE).alias("warning_critical"),
            (pl.col("protein_entropy_score") < ENTROPY_THRESHOLD).alias("warning_protein_entropy"),
        ]
    )

    customers = customers.with_columns(
        pl.struct(["match_recipes", "max_week_var"])
        .map_elements(
            lambda row: check_overlap(row, old_recipes, row["max_week_var"], target_week), return_dtype=pl.Boolean
        )
        .alias("warning_overlap")
    )

    return customers


def create_menu_feedback(payload: MenuFeedbackRequestModel) -> MenuFeedbackResponseModel:
    assert payload is not None, "Payload is None"

    target_year = payload.target_menu_week.menu_year
    target_week = payload.target_menu_week.menu_week
    company_id = payload.target_menu_week.company_id

    results = []
    for portion in payload.target_menu_week.portions:
        portion_id = portion.portion_id
        res = create_warnings(payload, target_week, company_id.upper(), portion_id)
        results.append(res)
    conc_results = pl.concat(results, how="vertical")

    conc_results = conc_results.filter(
        pl.col("warning_critical") | pl.col("warning_protein_entropy") | pl.col("warning_overlap")
    )

    response = response_from_stats(
        conc_results.select(
            [
                "company_id",
                "portion_id",
                "negative_taste_preferences",
                "number_of_users",
                "warning_critical",
                "warning_protein_entropy",
                "warning_overlap",
            ]
        ),
        target_year,
        target_week,
        company_id,
    )

    return response
