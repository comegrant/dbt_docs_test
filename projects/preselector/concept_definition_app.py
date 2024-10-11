import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import matplotlib.pyplot as plt
import polars as pl
import streamlit as st
from aligned import ContractStore, FeatureLocation, FileSource
from aligned.schemas.feature import Feature
from data_contracts.preselector.basket_features import BasketFeatures, PredefinedVectors
from preselector.main import load_menu_for, normalize_cost, run_preselector
from preselector.schemas.batch_request import GenerateMealkitRequest, YearWeek
from preselector.store import preselector_store
from ui.components.mealkit import mealkit
from ui.deeplinks.compare_week import cached_recipe_info


@dataclass
class Concept:
    name: str
    id: str


@dataclass
class FeatureImportance:
    target: float
    importance: float


async def load_attributes(company_id: str) -> list[Concept]:
    df: pl.DataFrame = (
        await PredefinedVectors.query()
        .select({"concept_name", "concept_id", "company_id", "vector_type"})
        .filter((pl.col("company_id") == company_id) & (pl.col("vector_type") == "importance"))
        .to_polars()
    ).sort("concept_name")

    return [Concept(name=row["concept_name"], id=row["concept_id"]) for row in df.to_dicts()]


def potential_features() -> list[Feature]:
    return list(BasketFeatures.compile().request_all.needed_requests[0].all_features)


async def existing_config_for(concept: Concept, company_id: str) -> dict[str, FeatureImportance]:
    df: pl.DataFrame = (
        await PredefinedVectors.query()
        .filter((pl.col("company_id") == company_id) & (pl.col("concept_id") == concept.id))
        .to_polars()
    )
    features = potential_features()

    importance = df.filter(pl.col("vector_type") == "importance").to_dicts()[0]
    target = df.filter(pl.col("vector_type") == "target").to_dicts()[0]

    return {
        feature.name: FeatureImportance(target=target[feature.name], importance=importance[feature.name])
        for feature in features
        if importance.get(feature.name) is not None
        and target.get(feature.name) is not None
        and importance[feature.name] != 0
    }


async def edit_concept(store: ContractStore) -> None:
    st.title("Edit Attribute Definition")

    companies = {
        "8A613C15-35E4-471F-91CC-972F933331D7": "Adams",
        "09ECD4F0-AE58-4539-8E8F-9275B1859A19": "Godt Levert",
        "6A2D0B60-84D6-4830-9945-58D518D27AC2": "Linas",
        "5E65A955-7B1A-446C-B24F-CFE576BF52D7": "RT",
    }
    with st.form("Select company"):
        company_id = st.selectbox(
            "Company",
            options=companies.keys(),
            format_func=lambda val: companies[val],
            index=None,
        )
        st.form_submit_button()

    if not company_id:
        return

    with st.spinner("Loading attributes"):
        concepts = await load_attributes(company_id)

    features = potential_features()

    if not features:
        raise ValueError("No features found")

    selected_concept = st.selectbox("Select a attribute", concepts, format_func=lambda x: x.name, index=None)

    if selected_concept is None:
        return

    existing_config = await existing_config_for(selected_concept, company_id)

    # if existing_config:
    #     st.header("Current attribute configuration")
    #     plot_importance_graph(
    #         pl.DataFrame(
    #             [
    #                 {
    #                     "feature": feat,
    #                     "importance": importance.importance,
    #                     "target": importance.target,
    #                 }
    #                 for feat, importance in existing_config.items()
    #             ]
    #         ).sort("importance", descending=False)
    #     )

    st.header("Edit concept configuration")
    selected_features = st.multiselect(
        "Features of interest",
        features,
        format_func=lambda x: x.name,
        default=[feature for feature in features if feature.name in existing_config],
    )

    new_config: dict[str, FeatureImportance] = defaultdict(lambda: FeatureImportance(0.0, 0.0))
    default_config = FeatureImportance(0.3, 0.3)

    with st.form(key="my_form"):
        for feature in selected_features:
            st.header(f"{feature.name.replace('_', ' ').capitalize()}")

            if feature.description:
                st.caption(f"**Description**: {feature.description}")

            st.caption(
                "Setting the importance to 0 means it is random, "
                "while 1 means the preselector will try to find a combination "
                "that is as close as possible to the target value."
            )

            new_config[feature.name].importance = st.slider(
                "Importance",
                value=existing_config.get(feature.name, default_config).importance,
                min_value=0.0,
                max_value=1.0,
                key=f"{feature.name}_importance",
            )

            st.caption(
                "Setting the target to 0 means it will be the lowest "
                "value for the given menu week. While 1 means the max value."
            )
            new_config[feature.name].target = st.slider(
                "Target value",
                value=existing_config.get(feature.name, default_config).target,
                min_value=0.0,
                max_value=1.0,
                key=f"{feature.name}_target",
            )

        should_save = st.form_submit_button(label="Refresh")

    if not (st.session_state.get("edit_was_submitted") or should_save):
        return

    st.session_state["edit_was_submitted"] = True

    features_targets = list(new_config.items())

    data = pl.DataFrame(
        [
            {
                "feature": feat,
                "importance": importance.importance,
                "target": importance.target,
            }
            for feat, importance in features_targets
        ]
    ).sort("importance", descending=False)

    st.header("New attribute configuration")
    plot_importance_graph(data)

    if not new_config:
        return

    if st.button("Save"):
        importance_row: dict = {
            feature.name: new_config[feature.name].importance if feature in selected_features else 0
            for feature in features
        }
        target_row: dict = {
            feature.name: new_config[feature.name].target if feature in selected_features else 0 for feature in features
        }
        importance_row["vector_type"] = "importance"
        target_row["vector_type"] = "target"
        vectors = pl.DataFrame([importance_row, target_row]).with_columns(
            concept_id=pl.lit(selected_concept.id),
            concept_name=pl.lit(selected_concept.name),
            company_id=pl.lit(company_id),
        )

        with st.spinner("Updating"):
            await PredefinedVectors.query().upsert(vectors)

    with st.form("View example mealkit"):
        now = datetime.now(tz=timezone.utc)
        year = st.number_input("Year", value=now.year, min_value=now.year)
        week = st.number_input(
            "Week",
            value=(now + timedelta(weeks=1)).isocalendar().week,
            min_value=1,
            max_value=52,
        )
        portion_size = st.selectbox("Portion Size", options=[2, 4, 6], index=1)
        number_of_recipes = st.number_input("Number of recipes", min_value=2, max_value=5, value=5)

        should_generate = st.form_submit_button("Generate Mealkit")

    if should_generate and portion_size:
        await view_mealkit(
            features=features,
            configuration=data,
            company_id=company_id,
            year=int(year),
            week=int(week),
            store=store,
            portion_size=int(portion_size),
            number_of_recipes=int(number_of_recipes),
        )


async def genreate_mealkit(store: ContractStore) -> None:
    st.title("Generate Mealkit")

    features = potential_features()

    if not features:
        raise ValueError("No features found")

    selected_features = st.multiselect(
        "Features of interest",
        features,
        format_func=lambda x: x.name,
    )
    new_config: dict[str, FeatureImportance] = defaultdict(lambda: FeatureImportance(1.0, 1.0))

    with st.form(key="my_form"):
        for feature in selected_features:
            st.header(f"{feature.name.replace('_', ' ').capitalize()}")

            if feature.description:
                st.caption(f"**Description**: {feature.description}")

            st.caption(
                "Setting the importance to 0 means it is random, "
                "while 1 means the preselector will try to find a combination "
                "that is as close as possible to the target value."
            )

            new_config[feature.name].importance = st.slider(
                "Importance",
                value=0.5,
                min_value=0.0,
                max_value=1.0,
                key=f"{feature.name}_importance",
            )

            st.caption(
                "Setting the target to 0 means it will be the lowest "
                "value for the given menu week. While 1 means the max value."
            )
            new_config[feature.name].target = st.slider(
                "Target value",
                value=0.5,
                min_value=0.0,
                max_value=1.0,
                key=f"{feature.name}_target",
            )

        should_update = st.form_submit_button(label="Generate")

    if not should_update:
        return

    data = pl.DataFrame(
        [
            {
                "feature": feat,
                "importance": importance.importance,
                "target": importance.target,
            }
            for feat, importance in new_config.items()
        ]
    ).sort("importance", descending=False)

    st.header("Mealkit configuration")
    plot_importance_graph(data)

    await view_mealkit(
        features=features,
        configuration=data,
        company_id="09ECD4F0-AE58-4539-8E8F-9275B1859A19",
        year=2024,
        week=29,
        store=store,
    )


async def view_mealkit(
    features: list[Feature],
    configuration: pl.DataFrame,
    company_id: str,
    year: int,
    week: int,
    store: ContractStore,
    portion_size: int = 4,
    number_of_recipes: int = 5,
) -> None:
    target_vector = pl.DataFrame([{row["feature"]: row["target"] for row in configuration.to_dicts()}])
    importance_vector = pl.DataFrame([{row["feature"]: row["importance"] for row in configuration.to_dicts()}])

    fill_columns = []
    for feature in features:
        if feature.name not in target_vector.columns:
            fill_columns.append(feature.name)

    target_vector = target_vector.with_columns([pl.lit(0).alias(col) for col in fill_columns])
    importance_vector = importance_vector.with_columns([pl.lit(0).alias(col) for col in fill_columns])

    normalized_importance = importance_vector  # (importance_vector / importance_vector.transpose().to_series().sum())

    feature_names = [feat.name for feat in features]
    importance_vector = importance_vector.with_columns(
        pl.col(feat) / pl.sum_horizontal(feature_names) for feat in feature_names
    )
    importance_vector = importance_vector.with_columns(mean_cost_of_food=pl.lit(1))

    customer = GenerateMealkitRequest(
        agreement_id=123,
        company_id=company_id,
        compute_for=[],
        concept_preference_ids=[],
        taste_preferences=[],
        portion_size=portion_size,
        number_of_recipes=number_of_recipes,
        override_deviation=False,
        has_data_processing_consent=False
    )

    st.write(
        f"Generating mealkit for week: {week} "
        f"with no taste preferences, {customer.portion_size} "
        f"protions and {customer.number_of_recipes} recipes."
    )

    with st.spinner("Loading menu"):
        menu = await load_menu_for(customer.company_id, year=year, week=week, store=store)

    with st.spinner("Caching features"):
        cache_store = await cache_normalized_recipes(year, week, store)

    with st.spinner("Loading mealkit CoF target"):
        cost_of_food = await (
            store.feature_view("one_sub_mealkits")
            .select({"cost_of_food_target_per_recipe"})
            .features_for(
                {
                    "company_id": [company_id],
                    "number_of_recipes": [number_of_recipes],
                    "number_of_portions": [portion_size],
                }
            )
            .to_polars()
        )

    raw_cost_of_food_value = cost_of_food["cost_of_food_target_per_recipe"].to_list()[0]
    assert raw_cost_of_food_value, (
        f"Missing cost_of_food target for {company_id},"
        f" n recipes: {number_of_recipes}, n portions: {portion_size}"
    )

    target_vector = await normalize_cost(
        year_week=YearWeek(week=week, year=year),
        mealkit_target_cost_of_food=raw_cost_of_food_value,
        request=customer,
        vector=target_vector,
        store=store,
    )

    with st.spinner("Running preselector"):
        output = await run_preselector(
            customer=customer,
            available_recipes=menu,
            recommendations=pl.DataFrame(),
            target_vector=target_vector,
            importance_vector=normalized_importance,
            store=cache_store,
        )

    with st.spinner("Loading recipe info"):
        recipe_info = await cached_recipe_info(output[0], year=year, week=week)

    mealkit(recipe_info, st) # type: ignore

async def cache_normalized_recipes(year: int, week: int, store: ContractStore) -> ContractStore:
    cache = FileSource.parquet_at(f"data/norm_recipe_features/{year}/{week}.parquet")

    await store.feature_view("normalized_recipe_features").all().filter(
        (pl.col("week") == week) & (pl.col("year") == year)
    ).write_to_source(cache)
    return store.update_source_for(
        FeatureLocation.feature_view("normalized_recipe_features"),
        cache
    )


def plot_importance_graph(data: pl.DataFrame) -> None:
    fig, ax = plt.subplots()

    percentage_values = (data["importance"] * 100).round()

    bars = ax.barh(
        data["feature"].to_list(),
        data["target"].clip_min(0.01).to_list(),
        color="#d84f4f",
    )

    # Set the alpha for each bar
    for bar, alpha in zip(bars, percentage_values.to_list(), strict=False):
        bar.set_alpha(alpha / 100)

    plt.xlim(0, 1)
    plt.xlabel("Target")
    plt.ylabel("Name")
    plt.title("Concept importance")
    plt.grid(axis="x", linestyle="--", alpha=0.7)

    st.pyplot(fig=fig)


async def missing_attributes() -> pl.DataFrame:
    companies = {
        "AMK": "8A613C15-35E4-471F-91CC-972F933331D7",
        "GL": "09ECD4F0-AE58-4539-8E8F-9275B1859A19",
        "LMK": "6A2D0B60-84D6-4830-9945-58D518D27AC2",
        "RT": "5E65A955-7B1A-446C-B24F-CFE576BF52D7",
    }

    default_values = {
        # Quick and easy
        "C28F210B-427E-45FA-9150-D6344CAE669B": {
            "cooking_time_mean": FeatureImportance(target=0.0, importance=1.0),
        },
        # Chef favorite
        "C94BCC7E-C023-40CE-81E0-C34DA3D79545": {
            "is_chef_choice_percentage": FeatureImportance(target=1.0, importance=1.0),
            "mean_number_of_ratings": FeatureImportance(target=0.75, importance=0.3),
            "mean_ratings": FeatureImportance(target=0.8, importance=0.6),
        },
        # Family
        "B172864F-D58E-4395-B182-26C6A1F1C746": {
            "is_family_friendly_percentage": FeatureImportance(target=1.0, importance=1.0),
        },
        # Vegetarion
        "6A494593-2931-4269-80EE-470D38F04796": {
            "is_vegetarian_percentage": FeatureImportance(target=1.0, importance=1.0),
        },
        # Low Cal
        "FD661CAD-7F45-4D02-A36E-12720D5C16CA": {
            "is_low_calorie": FeatureImportance(target=1.0, importance=1.0),
        },
        # Roede
        "DF81FF77-B4C4-4FC1-A135-AB7B0704D1FA": {
            "is_roede_percentage": FeatureImportance(target=1.0, importance=1.0),
        },
        # Singel
        "37CE056F-4779-4593-949A-42478734F747": {},
    }

    vector_types = ["importance", "target"]
    preferences = {
        "GL": {
            "Quick and Easy": "C28F210B-427E-45FA-9150-D6344CAE669B",
            "Chef's Favorite": "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "Single mealkit": "37CE056F-4779-4593-949A-42478734F747",
            "Family Friendly": "B172864F-D58E-4395-B182-26C6A1F1C746",
            "Roede mealkit": "DF81FF77-B4C4-4FC1-A135-AB7B0704D1FA",
            "Vegetarian": "6A494593-2931-4269-80EE-470D38F04796",
            "Low calorie": "FD661CAD-7F45-4D02-A36E-12720D5C16CA",
        },
        "AMK": {
            "Low calorie": "FD661CAD-7F45-4D02-A36E-12720D5C16CA",
            "Chef's Favorite": "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "Family Friendly": "B172864F-D58E-4395-B182-26C6A1F1C746",
            "Quick and Easy": "C28F210B-427E-45FA-9150-D6344CAE669B",
            "Vegetarian": "6A494593-2931-4269-80EE-470D38F04796",
        },
        "LMK": {
            "Vegetarian": "6A494593-2931-4269-80EE-470D38F04796",
            "Chef's Favorite": "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "Quick and easy": "C28F210B-427E-45FA-9150-D6344CAE669B",
            "Low calorie": "FD661CAD-7F45-4D02-A36E-12720D5C16CA",
            "Family Friendly": "B172864F-D58E-4395-B182-26C6A1F1C746",
        },
        "RT": {
            "Vegetarian": "6A494593-2931-4269-80EE-470D38F04796",
            "Chef's Favorite": "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "Quick and easy": "C28F210B-427E-45FA-9150-D6344CAE669B",
            "Family Friendly": "B172864F-D58E-4395-B182-26C6A1F1C746",
            "Low calorie": "FD661CAD-7F45-4D02-A36E-12720D5C16CA",
        },
    }

    features = potential_features()

    data_points: list[dict] = []

    for company_name, comp_id in companies.items():
        for vector_type in vector_types:
            for concept_name, concept_id in preferences[company_name].items():

                default_concept_values = default_values[concept_id]

                row: dict = {
                    # Select either importance or target
                    # From the defaults, either set it to 0.0
                    feature.name: getattr(
                        default_concept_values.get(feature.name, FeatureImportance(0.0, 0.0)),
                        vector_type
                    )
                    for feature in features
                }
                row["concept_id"] = concept_id
                row["concept_name"] = concept_name
                row["vector_type"] = vector_type
                row["company_id"] = comp_id
                data_points.append(row)

    df = pl.DataFrame(data_points)
    try:
        st.write(PredefinedVectors.metadata.source)
        existing = (
            await PredefinedVectors.query().select_columns(["company_id", "concept_id", "vector_type"]).to_polars()
        )
        return df.join(existing, how="left", on=["company_id", "concept_id"]).filter(
            pl.col("vector_type_right").is_null()
        )
    except: # noqa: E722
        return df

async def main() -> None:
    from aligned import FeatureLocation
    from preselector.materialize import materialize_data
    from tabs.tabs import run_active_tab

    logging.basicConfig(level=logging.INFO)

    store = preselector_store()

    async def _edit() -> None:
        await edit_concept(store)

    async def _generate() -> None:
        await genreate_mealkit(store)


    async def _fill_concepts() -> None:

        missing = await missing_attributes()
        st.write(missing.to_pandas())

        if st.button("Insert"):
            await PredefinedVectors.query().insert(missing.select(pl.exclude("vector_type_right")))

    async def _update_data() -> None:
        from data_contracts.preselector.store import Preselector

        locations = Preselector.query().view.source.depends_on()

        # Not updated yet
        ignore = FeatureLocation.feature_view("partitioned_recommendations")
        if ignore in locations:
            locations = locations - {ignore}

        with st.spinner("Materializing data"):
            await materialize_data(
                store,
                list(locations),
                logger=st.write,
                should_force_update=False
            )
            st.success("Updated all data")

    async def _migrate_to_new_features() -> None:

        if st.button("Migrate"):
            with st.spinner("Migrating"):

                df = await PredefinedVectors.query().all().to_polars()
                await store.overwrite(PredefinedVectors.location, df)


    await run_active_tab(
        [
            ("Edit Concept Definition", _edit),
            ("Generate Mealkit", _generate),
            ("Migrate", _migrate_to_new_features),
            ("Materialize", _update_data),
            ("Fill Concepts", _fill_concepts),
        ]
    )


if __name__ == "__main__":
    asyncio.run(main())
