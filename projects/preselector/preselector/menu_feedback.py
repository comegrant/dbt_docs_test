import math
from collections import Counter
from typing import Callable, Optional

import polars as pl
from catalog_connector import connection
from pydantic import BaseModel, alias_generators

AVAILABLE_PREFS = {
    "gluten",
    "lactose",
    "pork",
    "poultry",
    "beef",
    "lamb",
    "shellfish",
    "fish",
    "egg",
    "dairy",
    "non-vegetarian",
    "nuts",
}
AVAILABLE_PROTEINS = {
    "pork",
    "poultry",
    "beef",
    "lamb",
    "shellfish",
    "fish",
    "non-vegetarian",
}
VEGETARIAN_MAIN_INGREDIENT_ID = 3
PREVIOUS_WEEKS = 3
MAX_PORTION_SIZE = 5
ENTROPY_THRESHOLD = 0.75
MIN_PERC_USERS = 0.01


class ConfigModel(BaseModel):
    model_config = {
        "alias_generator": alias_generators.to_pascal,
        "populate_by_name": True,
    }


class PortionModel(ConfigModel):
    portion_id: int
    main_recipe_ids: list[int]


class MenuWeekModel(ConfigModel):
    menu_week: int
    menu_year: int
    company_id: str
    portions: list[PortionModel]


class MenuFeedbackRequestModel(ConfigModel):
    target_menu_week: MenuWeekModel
    previous_menu_weeks: list[MenuWeekModel]


class IssueModel(ConfigModel):
    issue_type: int
    issue_title: str
    amount_affected: float
    user_group_description: str
    user_group_id: str
    negative_preference_ids: Optional[list[str]]
    issue_description: str
    issue_recipe_ids: list[int] | str
    action_description: str
    action_recipe_ids: list[int] | None = []


class PortionFeedbackModel(ConfigModel):
    portion_id: int
    preference_groups: list[IssueModel]


class MenuWeekFeedbackModel(ConfigModel):
    menu_week: int
    menu_year: int
    company_id: str
    portions: list[PortionFeedbackModel]


class MenuFeedbackResponseModel(ConfigModel):
    target_menu_week: MenuWeekFeedbackModel
    status: str = "success"


def get_user_preferences(company_id: str) -> pl.DataFrame:
    """Get user preferences for a company."""

    df = connection.sql(
        f"""
        select
        company_id,
        negative_taste_preference_combo_id,
        negative_taste_preferences,
        negative_taste_preferences_ids,
        number_of_users
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
        main_recipe_id,
        recipe_main_ingredient_id,
        negative_taste_preferences,
        recipe_main_ingredient_name_english as recipe_main_ingredient_name
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
            "non-vegetarian" in user_preferences and recipe_main_ingredient_ids[i] == VEGETARIAN_MAIN_INGREDIENT_ID
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


def check_overlap_new(
    row: dict, old_recipes: dict[int, list[int]], max_var: int, target_week: int
) -> tuple[bool, set[int]]:
    if max_var == 0:
        return True, set(row["match_recipes"])
    elif max_var > PREVIOUS_WEEKS:
        return False, set()

    overlap_set = set(row["match_recipes"])

    for i in range(1, max_var + 1):
        previous_week = target_week - i
        week_recipes = old_recipes.get(previous_week, [])
        overlap_set = overlap_set & set(week_recipes)
        if not overlap_set:
            return False, set()

    return True, overlap_set


def is_missing_protein_variety(proteins: list[str], max_protein_var: int, gap_threshold: int = 2) -> bool:
    unique_count = len(set(proteins))
    return (max_protein_var - unique_count) >= gap_threshold


def is_skewed_distribution(proteins: list[str], max_protein_var: int, skew_ratio: float = 1.8) -> bool:
    if not proteins or max_protein_var <= 0:
        return False

    count = Counter(proteins)
    total = len(proteins)

    top_share = count.most_common(1)[0][1] / total
    uniform_share = 1 / max_protein_var

    return top_share >= (uniform_share * skew_ratio)


def build_critical_issue(row: dict) -> IssueModel:
    issue = f"There are only {row['n_recipes']} available recipes, meaning we cannot create a full mealkit."
    return IssueModel(
        issue_type=1,
        issue_title="Critical issue",
        amount_affected=row["perc_of_users"],
        user_group_description=f"Customers with negative preferences: {row['negative_taste_preferences']}.",
        user_group_id=row["negative_taste_preference_combo_id"],
        negative_preference_ids=row["preference_set_ids"],
        issue_description=issue,
        issue_recipe_ids=row["match_recipes"],
        action_description=f"Add {MAX_PORTION_SIZE-row['n_recipes']} new recipes that fulfill the preferences",
        action_recipe_ids=[],
    )


def build_overlap_issue(row: dict) -> IssueModel:
    issue = (
        f"There exists {row['n_recipes']} recipes for this user group. However, they are not rotated within "
        f"{row['max_week_var']+1} weeks potentially leading to repeat recipes"
    )
    return IssueModel(
        issue_type=3,
        issue_title="Week to week variation issue",
        amount_affected=row["perc_of_users"],
        user_group_description=f"Customers with negative preferences: {row['negative_taste_preferences']}.",
        user_group_id=row["negative_taste_preference_combo_id"],
        negative_preference_ids=row["preference_set_ids"],
        issue_description=issue,
        issue_recipe_ids=row["match_recipes"],
        action_description="Consider swapping out the repeated recipes.",
        action_recipe_ids=row["overlap_recipes"],
    )


def build_protein_issue(row: dict) -> IssueModel:
    proteins = row["match_ingredient_names"]
    max_var = row["max_protein_var"]
    avail_proteins = row["avail_proteins"]

    if is_missing_protein_variety(proteins, max_var):
        issue = f"The current recipes only contain {', '.join(set(proteins))} protein types."
        action = (
            f"Consider adding more {', '.join(set(avail_proteins) - set(proteins))} "
            "that suit the customers negative preferences."
        )
    elif is_skewed_distribution(proteins, max_var):
        protein_counts = Counter(proteins)
        total = sum(protein_counts.values())
        top_protein, top_count = protein_counts.most_common(1)[0]
        other_protein_shares = [
            f"{protein} ({count/total*100:.1f}%)" for protein, count in protein_counts.items() if protein != top_protein
        ]
        issue = (
            f"The protein distribution is heavily skewed, with {top_protein} "
            "appearing most frequently ({top_count/total*100:.1f}%)"
        )
        action = f"Consider adding more diversity in the lower-represented proteins: {', '.join(other_protein_shares)}."
    else:
        issue = "Protein variety is low, which may lead to reduced customer satisfaction."
        action = "Consider adding more diversity in protein types that suit the customers negative preferences."

    return IssueModel(
        issue_type=2,
        issue_title="Low protein diversity",
        amount_affected=row["perc_of_users"],
        user_group_description=f"Customers with negative preferences: {row['negative_taste_preferences']}.",
        user_group_id=row["negative_taste_preference_combo_id"],
        negative_preference_ids=row["preference_set_ids"],
        issue_description=issue,
        issue_recipe_ids=row["match_recipes"],
        action_description=action,
        action_recipe_ids=[],
    )


def determine_issue_types(row: dict) -> list[Callable[[dict], IssueModel]]:
    issues = []
    if row["warning_critical"]:
        issues.append(build_critical_issue)
    if row["warning_overlap"]:
        issues.append(build_overlap_issue)
    if row["warning_protein_entropy"]:
        issues.append(build_protein_issue)
    return issues


def build_preference_groups(row: dict) -> list[IssueModel]:
    issue_types = determine_issue_types(row)
    return [issue_builder(row) for issue_builder in issue_types]


def response_from_df(df: pl.DataFrame, year: int, week: int, company_id: str) -> MenuFeedbackResponseModel:
    portions = []
    for portion_id in df["portion_id"].unique():
        portion_df = df.filter(pl.col("portion_id") == portion_id)
        preference_groups = []
        for row in portion_df.iter_rows(named=True):
            issues = build_preference_groups(row)
            preference_groups.extend(issues)

        portion = PortionFeedbackModel(portion_id=portion_id, preference_groups=preference_groups)
        portions.append(portion)

    menu_week = MenuWeekFeedbackModel(menu_week=week, menu_year=year, company_id=company_id, portions=portions)

    return MenuFeedbackResponseModel(target_menu_week=menu_week)


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
    recipe_main_ingredient_names = recipes["recipe_main_ingredient_name"].to_list()

    customers = customers.with_columns(
        (pl.col("number_of_users") / pl.col("number_of_users").sum()).alias("perc_of_users")
    )

    customers = customers.with_columns(pl.lit(portion_id).alias("portion_id"))

    customers = customers.with_columns(
        pl.col("negative_taste_preferences")
        .map_elements(create_preference_set, return_dtype=pl.List(pl.Utf8))
        .alias("preference_set")
    )

    customers = customers.with_columns(
        pl.col("negative_taste_preferences_ids")
        .map_elements(lambda x: None if x is None else [str(item).strip() for item in x], return_dtype=pl.List(pl.Utf8))
        .alias("preference_set_ids")
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
            pl.col("preference_set")
            .map_elements(
                lambda x: find_matching_values(
                    user_preferences=set(x),
                    values=recipe_main_ingredient_names,
                    recipe_preferences=recipe_preferences,
                    recipe_main_ingredient_ids=recipe_main_ingredient_ids,
                ),
                return_dtype=pl.List(pl.Utf8),
            )
            .alias("match_ingredient_names"),
        ]
    )

    customers = customers.with_columns(
        pl.col("match_recipes").map_elements(len, return_dtype=pl.Int64).alias("n_recipes")
    )

    customers = customers.with_columns(
        (pl.col("n_recipes") < MAX_PORTION_SIZE).alias("warning_critical"),
    )

    customers = customers.with_columns((pl.col("n_recipes") // MAX_PORTION_SIZE).alias("max_week_var"))

    customers = customers.with_columns(
        [
            pl.struct(["match_recipes", "max_week_var"])
            .map_elements(
                lambda row: check_overlap_new(row, old_recipes, row["max_week_var"], target_week)[0],
                return_dtype=pl.Boolean,
            )
            .alias("warning_overlap"),
            pl.struct(["match_recipes", "max_week_var"])
            .map_elements(
                lambda row: list(check_overlap_new(row, old_recipes, row["max_week_var"], target_week)[1]),
                return_dtype=pl.List(pl.Int64),
            )
            .alias("overlap_recipes"),
        ]
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
                lambda x: list((set(x) - {"non-vegetarian"}) | {"vegetarian"}) if "non-vegetarian" in x else x,
                return_dtype=pl.List(pl.Utf8),
            )
            .alias("avail_prefs"),
            pl.col("avail_proteins")
            .map_elements(
                lambda x: list((set(x) - {"non-vegetarian"}) | {"vegetarian"}) if "non-vegetarian" in x else x,
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
        (pl.col("protein_entropy_score") < ENTROPY_THRESHOLD).alias("warning_protein_entropy")
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

    conc_results = conc_results.filter(pl.col("perc_of_users") >= MIN_PERC_USERS)

    response = response_from_df(
        conc_results,
        target_year,
        target_week,
        company_id,
    )

    return response
