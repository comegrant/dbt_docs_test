import logging

import polars as pl
from aligned import ContractStore
from data_contracts.orders import WeeksSinceRecipe

from preselector.schemas.batch_request import GenerateMealkitRequest

logger = logging.getLogger(__name__)


async def compute_weeks_ago(
    company_id: str, agreement_id: int, year_week: int, main_recipe_ids: list[int], store: ContractStore
) -> pl.DataFrame:
    """
    Computes the weeks ago based on the batch sources that exists.

    Args:
        company_id (str): The company id of the user
        agreement_id (int): The id of the user
        year_week (int): The week to generate the quarantining penalty for
        main_recipe_ids (list[int]): The recipes to fetch a value for
        store (ContractStore): The store containing the source to use

    Returns:
        pl.DataFrame: The dataframe containing the penalty data

    ```python
    store = ...

    df = await compute_weeks_ago(
        company_id="...",
        agreement_id=1312653,
        year_week=202514,
        main_recipe_ids=[1, 2, 3, ...],
        store=store
    )
    print(df)
    ```
    ┌──────────────┬──────────────┬─────────────┬─────────────┬─────────────┬────────────┬─────────────┐
    │ from_year_we ┆ last_order_y ┆ main_recipe ┆ company_id  ┆ agreement_i ┆ order_diff ┆ ordered_wee │
    │ ek           ┆ ear_week     ┆ _id         ┆ ---         ┆ d           ┆ ---        ┆ ks_ago      │
    │ ---          ┆ ---          ┆ ---         ┆ str         ┆ ---         ┆ f64        ┆ ---         │
    │ i64          ┆ i32          ┆ i32         ┆             ┆ i32         ┆            ┆ f64         │
    ╞══════════════╪══════════════╪═════════════╪═════════════╪═════════════╪════════════╪═════════════╡
    │ 202508       ┆ 202506       ┆ 51930       ┆ 8A613C15-35 ┆ 1312653     ┆ 2.0        ┆ 0.861654    │
    │              ┆              ┆             ┆ E4-471F-91C ┆             ┆            ┆             │
    │              ┆              ┆             ┆ C-972F93…   ┆             ┆            ┆             │
    └──────────────┴──────────────┴─────────────┴─────────────┴─────────────┴────────────┴─────────────┘
    """
    number_of_recipes = len(main_recipe_ids)
    req = store.feature_view(WeeksSinceRecipe).request
    derived_feature_names = sorted([feat.name for feat in req.derived_features])

    job = (
        store.feature_view(WeeksSinceRecipe)
        .features_for(
            {
                "agreement_id": [agreement_id] * number_of_recipes,
                "company_id": [company_id] * number_of_recipes,
                # Setting the from_year_week so it will be used when computing
                "from_year_week": [year_week] * number_of_recipes,
                "main_recipe_id": main_recipe_ids,
            }
        )
        .transform_polars(
            # Is currently a bug where this will not be compute
            # So need to remove it and then compute it again
            lambda df: df.select(pl.exclude(derived_feature_names)) if derived_feature_names[0] in df.columns else df
        )
        .derive_features()
    )

    return (await job.to_polars()).cast({"main_recipe_id": pl.Int32})


async def add_ordered_since_feature(
    customer: GenerateMealkitRequest,
    store: ContractStore,
    recipe_features: pl.DataFrame,
    year_week: int,
    selected_recipes: dict[int, int],
) -> pl.DataFrame:
    """
    Adds the recipe quarantining data.

    This means both historical orders, but also expected recipes in the customers selection.

    Args:
        customer (GenerateMealkitRequest): The customers generation request
        store (ContractStore): The store containing all the data sources
        recipe_features (pl.DataFrame): A dataframe containing all recipes candidates
        year_week (int): The year week to load the quarantining data based on. 1 week into the future?
        selected_recipes (dict[int, int]): A realtime input which defines when a recipes was selected

    Returns:
        pl.DataFrame: The recipe_features data frame with a new column containing the quarantining data for each recipe.
    """

    new_recipe_ids = recipe_features.filter(pl.col("main_recipe_id").is_in(selected_recipes.keys()).not_())[
        "main_recipe_id"
    ]

    selected_recipe_computation: pl.DataFrame | None = None
    weeks_since_recipe: pl.DataFrame | None = None

    if selected_recipes:
        manual_data = {
            "main_recipe_id": list(selected_recipes.keys()),
            "last_order_year_week": list(selected_recipes.values()),
            "company_id": [customer.company_id] * len(selected_recipes),
            "agreement_id": [customer.agreement_id] * len(selected_recipes),
            "from_year_week": [year_week] * len(selected_recipes),
        }
        selected_recipe_computation = (
            await store.feature_view(WeeksSinceRecipe).process_input(manual_data).to_polars()
        ).cast({"main_recipe_id": pl.Int32})

    if customer.agreement_id != 0:
        weeks_since_recipe = await compute_weeks_ago(
            company_id=customer.company_id,
            agreement_id=customer.agreement_id,
            year_week=year_week,
            main_recipe_ids=new_recipe_ids.to_list(),
            store=store,
        )

    return_columns = ["main_recipe_id", "ordered_weeks_ago"]
    default_value = 0

    if selected_recipe_computation is None and weeks_since_recipe is None:
        return recipe_features.with_columns(ordered_weeks_ago=pl.lit(default_value))
    elif selected_recipe_computation is not None and weeks_since_recipe is not None:
        return (
            recipe_features.cast({"main_recipe_id": pl.Int32})
            .join(
                weeks_since_recipe.select(return_columns).vstack(selected_recipe_computation.select(return_columns)),
                on="main_recipe_id",
                how="left",
            )
            .with_columns(pl.col("ordered_weeks_ago").fill_null(default_value))
        )
    elif selected_recipe_computation is not None:
        return (
            recipe_features.cast({"main_recipe_id": pl.Int32})
            .join(selected_recipe_computation.select(return_columns), on="main_recipe_id", how="left")
            .with_columns(pl.col("ordered_weeks_ago").fill_null(default_value))
        )
    elif weeks_since_recipe is not None:
        return (
            recipe_features.cast({"main_recipe_id": pl.Int32})
            .join(weeks_since_recipe.select(return_columns), on="main_recipe_id", how="left")
            .with_columns(pl.col("ordered_weeks_ago").fill_null(default_value))
        )
    else:
        raise ValueError("Should never happen")
