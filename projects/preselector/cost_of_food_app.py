import asyncio
from dataclasses import dataclass
from functools import partial

import matplotlib.pyplot as plt
import polars as pl
import streamlit as st
from aligned import Int32
from data_contracts.mealkits import OneSubMealkits
from data_contracts.preselector.store import Preselector
from data_contracts.recipe import RecipeCost
from streamlit.delta_generator import DeltaGenerator
from tabs.tabs import run_active_tab


async def main() -> None:
    st.set_page_config("Cost of Food")
    st.title("Cost of Food")

    new_recipe_chost = RecipeCost.with_schema(
        name="Test",
        source=RecipeCost.metadata.materialized_source, # type: ignore
        entities=dict(main_recipe_id=Int32(), portion_size=Int32(), menu_year=Int32(), menu_week=Int32()),
    )

    with st.form("Select company"):
        company_id = st.selectbox("Company", options=["09ECD4F0-AE58-4539-8E8F-9275B1859A19"])
        year = st.number_input("Year", value=2024)
        week = st.number_input("Week", value=33, min_value=1, max_value=52)

        st.form_submit_button("Compute")

    if not company_id or not year or not week:
        st.warning("Select a company")
        return

    with st.spinner("Loading selection"):
        selections = (
            await Preselector.query()
            .all()
            .filter((pl.col("company_id") == company_id) & (pl.col("year") == year) & (pl.col("week") == week))
            .to_polars()
        )

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
        cost = await new_recipe_chost.query().features_for(all_recipes).with_subfeatures().to_polars()

    cost_of_food_per_mealkit = (
        cost.group_by(["agreement_id", "number_of_recipes", "portion_size", "company_id"])
        .agg(
            delivered_cost_of_food=pl.col("recipe_cost_whole_units").sum(),
        )
        .with_columns(number_of_portions=pl.col("portion_size"))
    )

    mealkits_with_price = (
        await OneSubMealkits.query().features_for(cost_of_food_per_mealkit).with_subfeatures().to_polars()
    ).with_columns(deliverd_cof_perc=pl.col("delivered_cost_of_food") / pl.col("price"))

    group_by_keys = ["number_of_recipes", "portion_size"]

    overview = (
        mealkits_with_price.select(
            cost_of_food_percent=pl.col("delivered_cost_of_food").sum() / pl.col("price").sum(),
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

    target.metric("Target CoF", overview["target_cost_of_food"])
    estimated.metric(
        "Estimated CoF",
        overview["estimated_cost_of_food"],
        overview["estimated_cost_of_food"] - overview["target_cost_of_food"],
        delta_color="inverse",
    )
    percentage.metric("Percentage Differance", f'{overview["perc_diff"]:.2f}%')
    cof_percent.metric("CoF%", f'{overview["cost_of_food_percent"]:.2f}%')

    # pricy_recipes = cost.group_by("price_category_level").agg(
    #     pl.count()
    # )
    # st.write(pricy_recipes.to_pandas())

    async def absolute_cost_of_food(n_portions: int) -> None:
        raw_subset = mealkits_with_price.filter(pl.col("portion_size") == n_portions)
        agg_cost = raw_subset.group_by(group_by_keys).agg(
            selling_price=pl.col("price").sum().round().cast(pl.Int32),
            target_cost_of_food=pl.col("cost_of_food").sum().round().cast(pl.Int32),
            estimated_cost_of_food=pl.col("delivered_cost_of_food").sum().round().cast(pl.Int32),
            median_cost_of_food=pl.col("delivered_cost_of_food").median().round().cast(pl.Int32),
            mean_cost_of_food=pl.col("delivered_cost_of_food").mean().round().cast(pl.Int32),
            mean_target_cost_of_food=pl.col("cost_of_food").mean().round().cast(pl.Int32),
            number_of_mealkits=pl.count(),
        )
        subset = agg_cost.sort("number_of_recipes").select(pl.exclude(["portion_size"]))

        metrics = [CostOfFoodMetric(**row) for row in subset.to_dicts()]

        cols = st.columns(len(metrics))

        for metric, col in zip(metrics, cols, strict=False):
            portion_metrics(metric, col)

        for portion_size in raw_subset["number_of_recipes"].unique().to_list():
            st.subheader(f"CoF Distribution for {portion_size} recipes and {n_portions} portions")
            hist_subset = raw_subset.filter(pl.col("number_of_recipes") == portion_size)
            plt.hist(hist_subset["delivered_cost_of_food"].to_pandas())
            plt.axvline(x=float(hist_subset["cost_of_food"].median() or -1), label="Target CoF")  # type: ignore
            st.pyplot(plt.gcf())
            plt.close()

    portion_sizes = mealkits_with_price.select(pl.col("portion_size").unique()).to_series().to_list()

    await run_active_tab(
        [
            (f"{portion_size} Portions", partial(absolute_cost_of_food, n_portions=portion_size))
            for portion_size in portion_sizes
        ]
    )





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

    col.metric("Revenue", metrics.selling_price)
    col.metric("Target CoF", metrics.target_cost_of_food)
    col.metric(
        "Estimated CoF",
        metrics.estimated_cost_of_food,
        delta=metrics.estimated_cost_of_food - metrics.target_cost_of_food,
        delta_color="inverse",
    )
    col.metric("CoF%", f'{metrics.estimated_cost_of_food / metrics.selling_price:.2f}%')
    col.metric("Average CoF", metrics.mean_cost_of_food)
    col.metric("Target CoF per mealkit", metrics.mean_target_cost_of_food)
    # col.metric("Average CoF per recipe", metrics.mean_cost_of_food / metrics.number_of_recipes)
    # col.metric("Target CoF per recipe", metrics.mean_target_cost_of_food / metrics.number_of_recipes)

    col.metric("Median CoF", metrics.median_cost_of_food)
    col.metric("Mealkits", metrics.number_of_mealkits)


if __name__ == "__main__":
    asyncio.run(main())
