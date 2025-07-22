from datetime import datetime, timedelta, timezone

import polars as pl
from aligned import (
    Bool,
    ContractStore,
    CustomMethodDataSource,
    EventTimestamp,
    Int32,
    List,
    String,
    ValidFrom,
    feature_view,
)
from aligned.feature_store import FeatureViewStore, FeatureViewWrapper
from aligned.schemas.feature_view import RetrievalRequest
from project_owners.owner import Owner

from data_contracts.mealkits import DefaultMealboxRecipes
from data_contracts.sources import adb, adb_ml, materialized_data, ml_gold
from data_contracts.user import UserSubscription

flex_dish_id = "CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1"
mealbox_id = "2F163D69-8AC1-6E0C-8793-FF0000804EB3"

recommendation_engine_origin_id = "9e016a92-9e5c-4b5b-ac5d-739cefd6f07b".upper()
user_origin_id = "25017d0e-f788-48d7-8dc4-62581d58b698".upper()

contacts = [Owner.matsmoll().name]

historical_orders_sql = """
WITH velgandvrak AS (
    SELECT * FROM mb.products p
    WHERE p.product_type_id in ('CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1', '2f163d69-8ac1-6e0c-8793-ff0000804eb3')
    AND variation_portions != 1
),

data as (
    SELECT
        ba.agreement_id,
        ba.company_id,
        bao.delivery_date as delivered_at,
        p.variation_portions as portion_size,
        r.recipe_id,
        COALESCE(r.main_recipe_id, r.recipe_id) as main_recipe_id,
        rr.RATING as rating,
        wm.MENU_WEEK as week,
        wm.MENU_YEAR as year
    FROM cms.billing_agreement ba
             INNER JOIN cms.billing_agreement_order bao ON bao.agreement_id = ba.agreement_id
             INNER JOIN cms.billing_agreement_order_line baol ON baol.agreement_order_id = bao.id
             INNER JOIN velgandvrak p ON p.variation_id = baol.variation_id
             INNER JOIN pim.WEEKLY_MENUS wm
                ON wm.COMPANY_ID = ba.company_id
                    AND wm.MENU_WEEK = bao.week
                    AND wm.MENU_YEAR = bao.year
             INNER JOIN pim.MENUS m ON m.WEEKLY_MENUS_ID = wm.WEEKLY_MENUS_ID
             INNER JOIN pim.MENU_VARIATIONS mv ON mv.MENU_ID = m.MENU_ID
                AND mv.MENU_VARIATION_EXT_ID = baol.variation_id
             INNER JOIN pim.MENU_RECIPES mr ON mr.MENU_ID = m.menu_id
                AND mr.menu_recipe_order <= mv.menu_number_days
             INNER JOIN pim.recipes r ON r.recipe_id = mr.RECIPE_ID
             LEFT JOIN pim.RECIPES_RATING rr ON rr.RECIPE_ID = r.recipe_id
                AND rr.AGREEMENT_ID = ba.agreement_id
)

SELECT * FROM data WHERE year >= 2024
"""


@feature_view(
    name="historical_recipe_orders",
    description="The recipes that our customers have received. Together with the rating of the dish.",
    source=adb_ml.fetch(historical_orders_sql),
    materialized_source=materialized_data.parquet_at("historical_recipe_orders.parquet"),
    contacts=contacts,
    acceptable_freshness=timedelta(days=6),
    tags=["incremental"],
)
class HistoricalRecipeOrders:
    agreement_id = Int32().as_entity()
    recipe_id = Int32().as_entity()

    company_id = String()
    week = Int32()
    year = Int32()

    main_recipe_id = Int32()

    portion_size = Int32()

    delivered_at = ValidFrom()

    rating = (Int32().is_optional().lower_bound(0).upper_bound(5)).description(
        "A value of 0 means that the user did not make the dish. While a missing values means did not rate",
    )
    did_make_dish = rating != 0


@feature_view(
    name="current_selected_recipes",
    source=ml_gold.table("active_basket_deviations_recent_weeks"),
    materialized_source=materialized_data.partitioned_parquet_at(
        "current_selected_recipes", partition_keys=["company_id"]
    ),
)
class CurrentSelectedRecipes:
    billing_agreement_id = Int32().as_entity()
    company_id = String().as_entity()
    menu_week = Int32()
    menu_year = Int32()
    main_recipe_id = Int32()


quarantining_week_interval = 12


async def quarantined_recipes(request: RetrievalRequest, store: ContractStore | None = None) -> pl.LazyFrame:
    from datetime import date, timedelta

    def query(view_wrapper: FeatureViewWrapper) -> FeatureViewStore:
        """
        Makes it easier to swap between prod, and manually defined data for testing.
        """
        if store:
            return store.feature_view(view_wrapper.metadata.name)
        else:
            return view_wrapper.query()

    today = date.today()
    beginning_date = (today - timedelta(weeks=quarantining_week_interval)).isocalendar()
    beginning_yyyyww = beginning_date.year * 100 + beginning_date.week

    orders = (
        await query(HistoricalRecipeOrders)
        .filter(beginning_yyyyww <= pl.col("year") * 100 + pl.col("week"))
        .to_polars()
    )
    current_selection = (await query(CurrentSelectedRecipes).all().to_polars()).rename(
        {"menu_week": "week", "menu_year": "year", "billing_agreement_id": "agreement_id"}
    )
    subset = orders.select(current_selection.columns)

    return (
        subset.vstack(
            current_selection.cast(subset.schema)  # type: ignore
        )
        .with_columns(year_week=pl.col("year") * 100 + pl.col("week"))
        .filter(pl.col("year_week").is_not_null() & pl.col("main_recipe_id").is_not_null())
        .group_by(["agreement_id", "company_id", "main_recipe_id"])
        .agg(pl.col("year_week").max().alias("last_order_year_week"))
        .lazy()
    )


def week_to_date(col: str) -> pl.Expr:
    return pl.format("{}1", col).str.to_date("%G%V%w", strict=False)


@feature_view(
    name="weeks_since_recipe",
    source=CustomMethodDataSource.from_load(
        quarantined_recipes, depends_on={HistoricalRecipeOrders.location, CurrentSelectedRecipes.location}
    ),
    materialized_source=materialized_data.partitioned_parquet_at("weeks_since_recipe", partition_keys=["company_id"]),
)
class WeeksSinceRecipe:
    agreement_id = Int32().as_entity()
    company_id = String().as_entity()
    main_recipe_id = Int32().as_entity()

    from_year_week = Int32().is_optional().description("Needs to be sent in")
    last_order_year_week = Int32()

    order_diff = Int32().transformed_using_features_polars(
        using_features=[from_year_week, last_order_year_week],
        transformation=(week_to_date("from_year_week") - week_to_date("last_order_year_week")).dt.total_days() / 7,
    )
    ordered_weeks_ago = Int32().transformed_using_features_polars(
        using_features=[order_diff],
        transformation=(quarantining_week_interval - pl.col("order_diff")).clip(lower_bound=1).log(2)
        / pl.lit(quarantining_week_interval).log(2),
    )


deviation_sql = """SELECT
    r.recipe_id,
    bab.agreement_id,
    ba.company_id,
    babd.is_active,
    babd.week,
    babd.year,
    babd.origin,
    babdp.updated_by,
    babdp.created_by,
    babdp.updated_at
FROM cms.billing_agreement_basket_deviation AS babd
INNER JOIN cms.billing_agreement_basket_deviation_product AS babdp
    ON babd.id = babdp.billing_agreement_basket_deviation_id
INNER JOIN cms.billing_agreement_basket AS bab ON babd.billing_agreement_basket_id = bab.id
INNER JOIN cms.billing_agreement AS ba ON bab.agreement_id = ba.agreement_id
INNER JOIN pim.WEEKLY_MENUS wm
    ON wm.COMPANY_ID = ba.company_id
	AND wm.MENU_WEEK = babd.week
	AND wm.MENU_YEAR = babd.year
INNER JOIN pim.MENUS m ON m.WEEKLY_MENUS_ID = wm.WEEKLY_MENUS_ID
INNER JOIN pim.MENU_VARIATIONS mv
    ON mv.MENU_ID = m.MENU_ID AND mv.MENU_VARIATION_EXT_ID = babdp.subscribed_product_variation_id
INNER JOIN pim.MENU_RECIPES mr ON mr.MENU_ID = m.menu_id AND mr.menu_recipe_order <= mv.menu_number_days
INNER JOIN pim.recipes r ON r.recipe_id = mr.RECIPE_ID
WHERE babd.year >= 2024"""


@feature_view(
    name="raw_deviated_recipes",
    source=adb.fetch(deviation_sql),
    materialized_source=materialized_data.partitioned_parquet_at("deselected_recipes", partition_keys=["company_id"]),
)
class DeselectedRecipes:
    recipe_id = Int32().as_entity()
    agreement_id = Int32().as_entity()

    company_id = String()

    is_active = Bool()

    week = Int32()
    year = Int32()

    origin = String()

    created_by = String()
    updated_by = String()
    updated_at = EventTimestamp()


async def transform_deviations(req, limit) -> pl.LazyFrame:  # noqa: ANN001
    from data_contracts.mealkits import DefaultMealboxRecipes
    from data_contracts.user import UserSubscription

    subs = (
        await UserSubscription.query()
        .select_columns(["agreement_id", "subscribed_product_variation_id", "company_id"])
        .to_polars()
    )

    unique_users = subs["agreement_id"].unique()
    all_orders = await HistoricalRecipeOrders.query().all().to_lazy_polars()

    grouped_orders = all_orders.group_by(["agreement_id", "week", "year"]).agg(
        pl.col("recipe_id").alias("ordered_recipe_ids"),
    )

    default_boxes = (
        all_orders.filter(pl.col("agreement_id").is_in(unique_users))
        .select([pl.col("agreement_id"), pl.col("week"), pl.col("year")])
        .join(
            subs.select(["agreement_id", "subscribed_product_variation_id", "company_id"]).lazy(),
            on="agreement_id",
        )
        .rename(
            {
                "subscribed_product_variation_id": "variation_id",
                "week": "menu_week",
                "year": "menu_year",
            }
        )
    )

    default_baskets = (
        await DefaultMealboxRecipes.query()
        .select({"recipe_ids", "variation_name"})
        .all()
        .drop_invalid()
        .to_lazy_polars()
    ).with_columns(
        pl.col("menu_week").cast(pl.Int32),
        pl.col("menu_year").cast(pl.Int32),
    )

    grouped_orders = grouped_orders.join(
        default_boxes,
        left_on=["agreement_id", "week", "year"],
        right_on=["agreement_id", "menu_week", "menu_year"],
    ).collect()

    df = (
        grouped_orders.lazy()
        .join(
            default_baskets,
            left_on=["variation_id", "week", "year"],
            right_on=["variation_id", "menu_week", "menu_year"],
            how="left",
        )
        .with_columns(recipe_ids=pl.col("recipe_ids").fill_null([]))
        .collect()
    )

    df = (
        df.lazy()
        .with_columns(
            pl.col("ordered_recipe_ids").list.set_difference(pl.col("recipe_ids")).alias("selected_recipe_ids"),
            pl.col("recipe_ids").list.set_difference(pl.col("ordered_recipe_ids")).alias("deselected_recipe_ids"),
            pl.col("recipe_ids").alias("starting_recipe_ids"),
        )
        .unique(["agreement_id", "week", "year"])
    )

    df = (
        df.select(
            [
                "company_id",
                "agreement_id",
                "week",
                "year",
                "selected_recipe_ids",
                "deselected_recipe_ids",
                "starting_recipe_ids",
                "ordered_recipe_ids",
            ]
        )
        .with_columns(loaded_at=datetime.now(timezone.utc))
        .collect()
    )

    return df.lazy()


@feature_view(
    name="mealbox_changes",
    source=CustomMethodDataSource.from_methods(
        all_data=transform_deviations,
        depends_on_sources={UserSubscription.location, DefaultMealboxRecipes.location, HistoricalRecipeOrders.location},
    ),
    materialized_source=materialized_data.partitioned_parquet_at(
        "mealbox_changes", partition_keys=["company_id", "year"]
    ),
)
class MealboxChanges:
    agreement_id = Int32().as_entity()
    week = Int32().as_entity()
    year = Int32().as_entity()

    loaded_at = ValidFrom()

    company_id = String()

    selected_recipe_ids = List(Int32()).description("The customers selected recipe ids")
    deselected_recipe_ids = List(Int32()).description("The customers deselected recipe ids")
    starting_recipe_ids = List(Int32()).description("The customers starting recipes")
    ordered_recipe_ids = List(Int32()).description("The ordered recipes")


def mealbox_changes_as_rating(df: pl.LazyFrame) -> pl.LazyFrame:
    dislikes = (
        df.select(["deselected_recipe_ids", "agreement_id", "year", "week", "company_id"])
        .explode("deselected_recipe_ids")
        .with_columns(
            pl.col("deselected_recipe_ids").alias("recipe_id"),
            pl.lit(0).alias("rating"),
        )
        .filter(pl.col("recipe_id").is_not_null())
        .collect()
    )

    likes = (
        df.select(["selected_recipe_ids", "agreement_id", "year", "week", "company_id"])
        .explode("selected_recipe_ids")
        .with_columns(pl.col("selected_recipe_ids").alias("recipe_id"), pl.lit(4).alias("rating"))
        .filter(pl.col("recipe_id").is_not_null())
        .collect()
    )

    columns = ["company_id", "recipe_id", "agreement_id", "year", "week", "rating"]

    new = dislikes.select(columns).vstack(likes.select(columns)).with_columns(loaded_at=datetime.now(timezone.utc))
    return new.lazy()


@feature_view(
    name="mealbox_changes_as_rating",
    source=MealboxChanges.as_source().transform_with_polars(mealbox_changes_as_rating),
    materialized_source=materialized_data.parquet_at("mealbox_changes_as_rating.parquet"),
)
class MealboxChangesAsRating:
    agreement_id = Int32().as_entity()
    recipe_id = Int32().as_entity()

    week = Int32()
    year = Int32()

    loaded_at = ValidFrom()

    company_id = String()

    rating = Int32().lower_bound(0).upper_bound(5)


@feature_view(
    name="basket_deviation",
    source=adb.with_schema("cms").table("billing_agreement_basket_deviation"),
    materialized_source=materialized_data.delta_at("basket_deviation"),
    contacts=contacts,
)
class BasketDeviation:
    billing_agreement_basket_id = String().as_entity()
    week = Int32().as_entity()
    year = Int32().as_entity()

    created_by = String()
    updated_by = String()

    is_active = Bool()
    origin = String()

    was_user, was_meal_selector = origin.one_hot_encode(
        [user_origin_id, recommendation_engine_origin_id],
    )
