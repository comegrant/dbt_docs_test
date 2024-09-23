import asyncio
from dataclasses import dataclass
from datetime import date

import matplotlib.pyplot as plt
import polars as pl
import streamlit as st
from aligned import ContractStore, Int32
from data_contracts.mealkits import OneSubMealkits
from data_contracts.preselector.menu import CostOfFoodPerMenuWeek
from data_contracts.preselector.store import Preselector
from data_contracts.recipe import RecipeCost
from preselector.store import preselector_store
from streamlit.delta_generator import DeltaGenerator
from tabs.tabs import tab_index


def format_number(number: float) -> str:
    return f"{number:,.0f}".replace(",", " ")

async def display_cost(company_id: str, year: int, week: int, store: ContractStore, portion_size: int) -> None:

    st.subheader("Simulated Cost of Food")
    new_recipe_cost = RecipeCost.with_schema(
        name="Test",
        source=RecipeCost.metadata.materialized_source, # type: ignore
        entities=dict(main_recipe_id=Int32(), portion_size=Int32(), menu_year=Int32(), menu_week=Int32()),
    )

    mealkit_join_keys = ["company_id", "number_of_recipes", "number_of_portions"]
    try:
        with st.spinner("Loading selection"):
            selections = (
                await store.feature_view(Preselector).filter(
                    (pl.col("company_id") == company_id)
                    & (pl.col("year") == year)
                    & (pl.col("week") == week)
                )
                .unique_on(["agreement_id"], sort_key="generated_at")
                .to_polars()
            )
    except: # noqa: E722
        selections = pl.DataFrame()

    if selections.is_empty():
        st.warning(f"Found no data for year week: {year}-{week} in company {company_id}")
        return

    all_recipes = (
        selections.select(pl.exclude("variation_ids"))
        .with_columns(number_of_recipes=pl.col("main_recipe_ids").list.len())
        .explode("main_recipe_ids")
        .with_columns(
            menu_year=pl.col("year"),
            menu_week=pl.col("week"),
            main_recipe_id=pl.col("main_recipe_ids"),
        )
    )

    with st.spinner("Loading cost"):
        view_store = new_recipe_cost.query()
        cost = (
            await view_store.select({
                feature.name for feature in
                view_store.view.full_schema
                if feature.name != "is_plus_portion"
            }).features_for(all_recipes).with_subfeatures().to_polars()
        ).with_columns(number_of_portions=pl.col("portion_size")).cast({
            "number_of_recipes": pl.UInt8,
            "number_of_portions": pl.UInt8
        })

    cost_of_food_per_mealkit = (
        cost.group_by(["agreement_id", "number_of_recipes", "number_of_portions", "company_id"])
        .agg(
            delivered_cost_of_food=pl.col("recipe_cost_whole_units").sum(),
        )
    )

    mealkit_targets = (await store.feature_view(OneSubMealkits).filter(
        pl.col("company_id") == company_id
    ).to_polars()).cast({
        "number_of_recipes": pl.UInt8,
        "number_of_portions": pl.UInt8
    })
    group_by_keys = ["number_of_recipes", "number_of_portions"]

    mealkits_with_price = (
        cost_of_food_per_mealkit.join(
            mealkit_targets,
            left_on=mealkit_join_keys,
            right_on=mealkit_join_keys,
        )
    ).with_columns(deliverd_cof_perc=pl.col("delivered_cost_of_food") / pl.col("price"))

    overview = (
        mealkits_with_price.select(
            cost_of_food_percent=pl.col("delivered_cost_of_food").sum() * 100 / pl.col("price").sum(),
            estimated_cost_of_food=pl.col("delivered_cost_of_food").sum().round().cast(pl.Int32),
            target_cost_of_food=pl.col("cost_of_food").sum().round().cast(pl.Int32),
        )
        .with_columns(
            perc_diff=(pl.col("estimated_cost_of_food") - pl.col("target_cost_of_food"))
            / pl.col("target_cost_of_food")
            * 100
        )
        .to_dicts()[0]
    )

    target, estimated, percentage, cof_percent = st.columns(4)

    target.metric("Target CoF", format_number(overview["target_cost_of_food"]))
    estimated.metric(
        "Estimated CoF",
        format_number(overview["estimated_cost_of_food"]),
        format_number(overview["estimated_cost_of_food"] - overview["target_cost_of_food"]),
        delta_color="inverse",
    )
    percentage.metric("Percentage Differance", f'{overview["perc_diff"]:.2f}%')
    cof_percent.metric("CoF%", f'{overview["cost_of_food_percent"]:.2f}%')
    portion_sizes = mealkits_with_price.select(pl.col("number_of_portions").unique()).to_series().to_list()

    async def absolute_cost_of_food(n_portions: int) -> None:
        raw_subset = mealkits_with_price.filter(pl.col("number_of_portions") == n_portions)
        agg_cost = raw_subset.group_by(group_by_keys).agg(
            selling_price=pl.col("price").sum().round().cast(pl.Int32),
            target_cost_of_food=pl.col("cost_of_food").sum().round().cast(pl.Int32),
            estimated_cost_of_food=pl.col("delivered_cost_of_food").sum().round().cast(pl.Int32),
            median_cost_of_food=pl.col("delivered_cost_of_food").median().round().cast(pl.Int32),
            mean_cost_of_food=pl.col("delivered_cost_of_food").mean().round().cast(pl.Int32),
            mean_target_cost_of_food=pl.col("cost_of_food").mean().round().cast(pl.Int32),
            number_of_mealkits=pl.count(),
        )

        subset = agg_cost.sort("number_of_recipes").select(pl.exclude(["number_of_portions"]))

        metrics = [CostOfFoodMetric(**row) for row in subset.to_dicts()]

        cols = st.columns(len(metrics))

        for metric, col in zip(metrics, cols, strict=False):
            portion_metrics(metric, col)

        for index, n_recipes in enumerate(raw_subset["number_of_recipes"].unique().to_list()):

            with cols[index].container():
                st.subheader(f"CoF Distribution for {n_recipes} recipes and {n_portions} portions")
                hist_subset = raw_subset.filter(pl.col("number_of_recipes") == n_recipes)
                plt.hist(hist_subset["delivered_cost_of_food"].to_pandas())
                plt.axvline(x=float(hist_subset["cost_of_food"].median() or -1), label="Target CoF")  # type: ignore
                st.pyplot(plt.gcf())
                plt.close()

    if not portion_sizes:
        st.warning("Found no portion sizes")
        return

    await absolute_cost_of_food(n_portions=portion_size)

async def set_cost_of_food(company_id: str, year: int, week: int, store: ContractStore, n_portions: int) -> None:
    st.subheader("Update Cost of Food")

    number_of_recipes = [ 3, 4, 5 ]

    entities: dict[str, list] = {
        "number_of_portions": [],
        "number_of_recipes": [],
        "company_id": [company_id] * (len(number_of_recipes))
    }
    for n_recipes in number_of_recipes:
        entities["number_of_portions"].append(n_portions)
        entities["number_of_recipes"].append(n_recipes)

    original_targets = await store.feature_view(OneSubMealkits).features_for(entities).to_polars()

    try:
        year_week_cof = await store.feature_view(CostOfFoodPerMenuWeek).features_for(
            original_targets.with_columns(
                year=pl.lit(year),
                week=pl.lit(week)
            )
        ).to_polars()

        year_week_cof = year_week_cof.join(
            original_targets,
            on=["number_of_recipes", "number_of_portions"],
            how="left",
            coalesce=False
        ).with_columns(
            pl.col("target_cost_of_food").fill_null(pl.col("cost_of_food"))
        )

    except: # noqa: E722
        year_week_cof = original_targets.with_columns(
            year=pl.lit(year),
            week=pl.lit(week),
            target_cost_of_food=pl.col("cost_of_food")
        )


    portions = year_week_cof.filter(
        pl.col("number_of_portions") == n_portions
    )

    async def write(new_values: dict) -> None:
        with st.spinner("Saving"):
            await store.feature_view(CostOfFoodPerMenuWeek).upsert(new_values)
            st.rerun()


    @st.fragment
    def update_cost(current: pl.DataFrame, default: pl.DataFrame) -> None:

        assert default.height == 1, f"Expected 1 got {default.height}"
        assert current.height == 1, f"Expected 1 got {current.height}"


        default_values = default.to_dicts()[0]
        values = current.to_dicts()[0]

        cof_perc = values["target_cost_of_food"] * 100 / default_values["price"]
        target = float(values["target_cost_of_food"])

        left, right = st.columns(2)

        left.metric("Target margin %", f"{cof_perc:,.2f}%".replace(",", " "))
        right.metric("Target margin", f"{target:,.0f}".replace(",", " "))

        new_value = float(
            st.number_input(
                "Cost of Food Target",
                value=target,
                key=f"org-{target}-in",
                min_value=0.0,
                step=1.0
            )
        )

        new_cof_perc = new_value * 100 / default_values["price"]
        new_delta = new_value - target
        new_delta_perc = (new_value - target) * 100 / default_values["price"]

        if new_value != target:
            left, right = st.columns(2)
            left.metric(
                "Updated CoF %",
                f"{new_cof_perc:,.2f}%".replace(",", " "),
                delta=f"{new_delta_perc:,.2f}% change".replace(",", " "),
                delta_color="inverse"
            )
            right.metric(
                "Updated target",
                f"{new_value:,.0f}".replace(",", " "),
                delta=f"{new_delta:,.0f} change".replace(",", " "),
                delta_color="inverse"
            )

            if st.button("Update", key=f"org-{target}"):
                values["target_cost_of_food"] = new_value
                asyncio.run(write(values))

    for index, col in enumerate(st.columns(len(number_of_recipes))):
        n_recipes = number_of_recipes[index]
        data = portions.filter(pl.col("number_of_recipes") == n_recipes)
        original_target = original_targets.filter(
            pl.col("number_of_recipes") == n_recipes,
            pl.col("number_of_portions") == n_portions
        )

        with col:
            st.write(f"### {n_recipes} recipes")
            update_cost(data, original_target)


def dummy_store(store: ContractStore) -> ContractStore:
    from aligned.data_source.batch_data_source import DummyDataSource
    from aligned.feature_source import BatchFeatureSource

    assert isinstance(store.feature_source, BatchFeatureSource)
    assert isinstance(store.feature_source.sources, dict)

    for source_name in store.feature_source.sources:
        store.feature_source.sources[source_name] = DummyDataSource()

    return store


async def main() -> None:
    st.set_page_config("Cost of Food", layout="wide")
    st.title("Cost of Food")

    store = preselector_store()

    today = date.today()

    company_map = {
        "09ECD4F0-AE58-4539-8E8F-9275B1859A19": "godtlevert",
        "8A613C15-35E4-471F-91CC-972F933331D7": "adams",
        "6A2D0B60-84D6-4830-9945-58D518D27AC2": "linas",
        "5E65A955-7B1A-446C-B24F-CFE576BF52D7": "retnemt",
    }
    with st.form("Select company"):
        company_id = st.selectbox(
            "Company",
            options=company_map.keys(),
            format_func=lambda key: company_map[key]
        )
        year = st.number_input("Year", value=today.year)
        week = st.number_input("Week", value=today.isocalendar().week + 5, min_value=1, max_value=52)

        st.form_submit_button("Compute")

    if not company_id or not year or not week:
        st.warning("Select a company")
        return

    portion_sizes = [ 2, 4, 6 ]
    if company_id == "09ECD4F0-AE58-4539-8E8F-9275B1859A19":
        portion_sizes = [1, *portion_sizes]

    selected_portion_size = select_portion_size(portion_sizes)
    await set_cost_of_food(company_id, int(year), int(week), store, selected_portion_size)
    await display_cost(company_id, int(year), int(week), store, selected_portion_size)


def select_portion_size(available_portions: list[int]) -> int:
    selected_portion_index = tab_index([
        f"{n_portion} Portions"
        for n_portion in available_portions
    ])
    return available_portions[selected_portion_index]


@dataclass
class CostOfFoodMetric:
    number_of_recipes: int
    selling_price: int
    target_cost_of_food: int
    estimated_cost_of_food: int
    median_cost_of_food: int
    mean_cost_of_food: int
    mean_target_cost_of_food: int
    number_of_mealkits: int


def portion_metrics(metrics: CostOfFoodMetric, col: DeltaGenerator) -> None:
    col.caption(f"{metrics.number_of_recipes} recipes")


    col.metric("Revenue", format_number(metrics.selling_price))
    col.metric("Target CoF", format_number(metrics.target_cost_of_food))
    col.metric(
        "Estimated CoF",
        format_number(metrics.estimated_cost_of_food),
        delta=format_number(metrics.estimated_cost_of_food - metrics.target_cost_of_food),
        delta_color="inverse",
    )
    col.metric("CoF%", f'{metrics.estimated_cost_of_food * 100 / metrics.selling_price:,.2f}%'.replace(",", " "))
    col.metric("Average CoF", metrics.mean_cost_of_food)
    col.metric("Target CoF per mealkit", metrics.mean_target_cost_of_food)
    # col.metric("Average CoF per recipe", metrics.mean_cost_of_food / metrics.number_of_recipes)
    # col.metric("Target CoF per recipe", metrics.mean_target_cost_of_food / metrics.number_of_recipes)

    col.metric("Median CoF", metrics.median_cost_of_food)
    col.metric("Mealkits", format_number(metrics.number_of_mealkits))


if __name__ == "__main__":
    asyncio.run(main())
