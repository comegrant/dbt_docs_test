from datetime import timedelta

import polars as pl
from aligned import (
    Bool,
    ContractStore,
    CustomMethodDataSource,
    Embedding,
    EventTimestamp,
    Float,
    Int32,
    List,
    String,
    Timestamp,
    feature_view,
    model_contract,
)
from aligned.exposed_model.interface import openai_embedding
from aligned.feature_store import FeatureViewStore
from aligned.feature_view.feature_view import FeatureViewWrapper
from aligned.retrival_job import RetrivalJob
from aligned.schemas.feature_view import RetrivalRequest
from aligned.sources.in_mem_source import InMemorySource
from project_owners.owner import Owner

from data_contracts.sources import adb, adb_ml, materialized_data, pim_core

contacts = [Owner.matsmoll().markdown(), Owner.niladri().markdown()]

taxonomies_sql = """
SELECT recipe_id, recipe_taxonomies, GETDATE() as loaded_at
FROM (SELECT r.recipe_id,
           tt.language_id,
           STRING_AGG(lower(trim(tt.TAXONOMIES_NAME)), ',') as recipe_taxonomies,
           MAX(COALESCE(t.modified_date, t.created_date))   as updated_at
    FROM pim.recipes_taxonomies rt
             INNER JOIN pim.TAXONOMIES t ON t.taxonomies_id = rt.taxonomies_id
             INNER JOIN pim.taxonomies_translations tt ON tt.taxonomies_id = rt.taxonomies_id
             INNER JOIN pim.recipes r ON r.recipe_id = rt.RECIPE_ID
    WHERE t.status_code_id = 1           -- Active
      AND t.taxonomy_type IN (1, 11, 12) -- Recipe
      AND r.main_recipe_id IS NOT NULL
    GROUP BY r.recipe_id, tt.language_id
) tax
"""

recipe_ingredients_sql = """
SELECT
    recipe_id,
    CONCAT('["', STRING_AGG(REPLACE(ingredient_name, '"', ''), '","'), '"]') as all_ingredients,
    MAX(created_at) as loaded_at
FROM (
    SELECT rp.RECIPE_ID as recipe_id, it.INGREDIENT_NAME as ingredient_name, i.created_date as created_at
    FROM pim.RECIPE_PORTIONS rp
    INNER JOIN pim.PORTIONS p on p.PORTION_ID = rp.PORTION_ID
    INNER JOIN pim.CHEF_INGREDIENT_SECTIONS cis ON cis.recipe_portion_id = rp.recipe_portion_id
    INNER JOIN pim.chef_ingredients ci ON ci.chef_ingredient_section_id = cis.chef_ingredient_section_id
    INNER JOIN pim.order_ingredients oi ON oi.order_ingredient_id = ci.order_ingredient_id
    INNER JOIN pim.ingredients i on i.ingredient_internal_reference = oi.INGREDIENT_INTERNAL_REFERENCE
    INNER JOIN pim.INGREDIENTS_TRANSLATIONS it ON it.INGREDIENT_ID = i.ingredient_id
    INNER JOIN pim.suppliers s ON s.supplier_id = i.supplier_id
    WHERE rp.CREATED_DATE > '2023-01-01' AND s.supplier_name != 'Basis'
) as ingredients
GROUP BY recipe_id
"""

recipe_features_sql = """WITH taxonomies AS (
    SELECT
        rt.RECIPE_ID as recipe_id,
        CONCAT('["', STRING_AGG(tt.TAXONOMIES_NAME, '", "'), '"]') as taxonomies,
        CONCAT('["', STRING_AGG(tt.TAXONOMIES_ID, '", "'), '"]') as taxonomy_ids
    FROM pim.TAXONOMIES_TRANSLATIONS tt
    INNER JOIN pim.RECIPES_TAXONOMIES rt on rt.TAXONOMIES_ID = tt.TAXONOMIES_ID
    INNER JOIN pim.taxonomies t ON t.taxonomies_id = tt.TAXONOMIES_ID
    WHERE t.taxonomy_type IN ('1', '10', '11', '12', '19')
    GROUP BY rt.RECIPE_ID
)

SELECT *
FROM (SELECT rec.recipe_id,
             COALESCE(rec.main_recipe_id, rec.recipe_id) as main_recipe_id,
             rec.recipes_year as year,
             rec.recipes_week as week,
             rc.company_id,
             rra.number_of_ratings,
             rra.average_rating,
             rm.RECIPE_MAIN_INGREDIENT_ID as main_ingredient_id,
             rm.RECIPE_PHOTO as recipe_photo,
             rm.COOKING_TIME_FROM as cooking_time_from,
             rm.COOKING_TIME_TO as cooking_time_to,
             rmt.recipe_name,
             tx.taxonomies,
             tx.taxonomy_ids,
             rm.MODIFIED_DATE as updated_at,
             GETDATE() as loaded_at,
             ROW_NUMBER() over (PARTITION BY rec.recipe_id ORDER BY rmt.language_id) as nr
      FROM pim.recipes rec
        LEFT JOIN pim.recipe_rating_average rra ON rra.main_recipe_id = rec.main_recipe_id
	INNER JOIN pim.RECIPE_COMPANIES rc ON rc.RECIPE_ID = rec.recipe_id
        INNER JOIN pim.recipes_metadata rm ON rec.recipe_metadata_id = rm.RECIPE_METADATA_ID
        INNER JOIN pim.recipe_metadata_translations rmt ON rmt.recipe_metadata_id = rec.recipe_metadata_id
        INNER JOIN taxonomies tx ON tx.recipe_id = rec.recipe_id
        WHERE rec.is_active = 1
) as recipes
WHERE recipes.nr = 1"""


main_ingredients_in_recipe = """SELECT *
FROM (
        SELECT
	rm.recipe_id as recipe_id,
	COALESCE(r.main_recipe_id, r.recipe_id) as main_recipe_id,
	it.INGREDIENT_ID as ingredient_id,
	it.INGREDIENT_NAME as ingredient_name,
	ic.INGREDIENT_CATEGORY_ID as ingredient_category_id,
	oi.IS_MAIN_CARBOHYDRATE as is_main_carbohydrate,
	oi.IS_MAIN_PROTEIN as is_main_protein,
	i.created_date as created_at,
	ROW_NUMBER() over (PARTITION BY rm.recipe_id, oi.IS_MAIN_PROTEIN ORDER BY rp.PORTION_ID) as nr
	FROM pim.RECIPE_PORTIONS rp
	INNER JOIN pim.recipes r on rp.RECIPE_ID = r.recipe_id AND r.main_recipe_id IS NULL
	INNER JOIN pim.recipes rm on r.recipe_id = COALESCE(rm.main_recipe_id, rm.recipe_id)
	INNER JOIN pim.PORTIONS p on p.PORTION_ID = rp.PORTION_ID
	INNER JOIN pim.CHEF_INGREDIENT_SECTIONS cis ON cis.recipe_portion_id = rp.recipe_portion_id
	INNER JOIN pim.chef_ingredients ci ON ci.chef_ingredient_section_id = cis.chef_ingredient_section_id
	INNER JOIN pim.order_ingredients oi ON oi.order_ingredient_id = ci.order_ingredient_id
	INNER JOIN pim.ingredients i on i.ingredient_internal_reference = oi.INGREDIENT_INTERNAL_REFERENCE
	INNER JOIN pim.INGREDIENTS_TRANSLATIONS it ON it.INGREDIENT_ID = i.ingredient_id
	INNER JOIN pim.INGREDIENT_CATEGORIES ic ON i.ingredient_category_id = ic.INGREDIENT_CATEGORY_ID
	INNER JOIN pim.INGREDIENT_CATEGORIES_TRANSLATIONS ict ON ict.INGREDIENT_CATEGORY_ID = ic.INGREDIENT_CATEGORY_ID
	WHERE (oi.IS_MAIN_CARBOHYDRATE = '1' OR oi.IS_MAIN_PROTEIN = '1')
) as recipe
WHERE nr = 1
ORDER BY RECIPE_ID DESC"""

ingredient_categories = """SELECT *
FROM (
    select
        ic.INGREDIENT_CATEGORY_ID as ingredient_category_id,
        ic.PARENT_CATEGORY_ID as parent_category_id,
        ict.INGREDIENT_CATEGORY_NAME as ingredient_category_name,
        ict.INGREDIENT_CATEGORY_DESCRIPTION as category_description,
        ROW_NUMBER() over (PARTITION BY ic.INGREDIENT_CATEGORY_ID ORDER BY ict.LANGUAGE_ID) as nr
    FROM pim.INGREDIENT_CATEGORIES ic
    INNER JOIN pim.INGREDIENT_CATEGORIES_TRANSLATIONS ict ON ic.INGREDIENT_CATEGORY_ID = ict.INGREDIENT_CATEGORY_ID
    WHERE STATUS_CODE_ID = 1
) as cat
WHERE nr = 1
"""

def join_root_ingredient_category(df: pl.LazyFrame) -> pl.LazyFrame:

    parent_cat_col = "parent_category_id"
    ingred_cat_col = "ingredient_category_id"
    inter_col = "inter_col"
    cat_name_col = "ingredient_category_name"
    root_cat_name_col = "root_category_name"
    cat_desc = "category_description"

    root_col = "root_category_id"

    target_group = 'CATEGORY GROUP'

    all_categories = df.collect().with_columns(
        pl.col(parent_cat_col).cast(pl.Int64)
    ).with_columns(
        pl.when(
            pl.col(cat_desc).is_null() | (pl.col(cat_desc) != target_group)
        ).then(
            pl.col(parent_cat_col)
        ).otherwise(
            pl.lit(None)
        ).alias(parent_cat_col)
    )

    root_categories = all_categories.filter(
        pl.col(parent_cat_col).is_null()
    ).select(
        pl.col(ingred_cat_col),
        pl.col(cat_name_col),
        pl.col(cat_desc),
        pl.col(cat_name_col).alias(root_cat_name_col),
        pl.col(ingred_cat_col).alias(root_col),
    ).filter(
        pl.col(cat_desc) == target_group
    )

    join_parents = all_categories.filter(pl.col(parent_cat_col).is_not_null())


    # Join as long as there exist a parent id
    while not join_parents.is_empty():
        with_parent = join_parents.join(
            all_categories,
            left_on=parent_cat_col,
            right_on=ingred_cat_col,
            suffix="_right",
            how="left",
        ).select(
            pl.col(ingred_cat_col),
            pl.col(cat_name_col),
            pl.col(parent_cat_col).alias(inter_col),
            pl.col(f"{parent_cat_col}_right").alias(parent_cat_col),
            pl.col(f"{cat_name_col}_right").alias(root_cat_name_col),
            pl.col(f"{cat_desc}_right").alias(cat_desc)
        )

        roots = with_parent.filter(
            pl.col(parent_cat_col).is_null()
        ).with_columns(
            pl.col(inter_col).alias(root_col)
        )
        root_categories = root_categories.vstack(roots.select(root_categories.columns))

        join_parents = with_parent.filter(
            pl.col(parent_cat_col).is_not_null()
        ).select(
            pl.col(ingred_cat_col),
            pl.col(cat_name_col),
            pl.col(parent_cat_col),
            pl.col(cat_desc)
        )

    return root_categories.filter(pl.col(cat_desc) == target_group).lazy()


@feature_view(
    name="raw_ingredient_categories",
    source=adb.fetch(ingredient_categories),
    materialized_source=materialized_data.parquet_at("raw_ingredient_categories.parquet")
)
class RawIngredientCategories:
    ingredient_category_id = Int32().as_entity()
    ingredient_category_name = String()
    parent_category_id = Int32()
    category_description = String()


@feature_view(
    name="ingredient_category_groups",
    source=RawIngredientCategories.as_source().transform_with_polars(join_root_ingredient_category),
    materialized_source=materialized_data.parquet_at("ingredient_category_group.parquet")
)
class IngredientCategories:
    ingredient_category_id = Int32().as_entity()
    ingredient_category_name = String()

    root_category_id = Int32()
    root_category_name = String()
    category_description = String()


@feature_view(
    name="main_ingredient_in_recipe",
    source=adb.fetch(main_ingredients_in_recipe),
    materialized_source=materialized_data.parquet_at("main_ingredient_in_recipe.parquet")
)
class MainIngredients:
    recipe_id = Int32().as_entity()
    main_recipe_id = Int32()

    ingredient_name = String()
    ingredient_id = Int32()
    ingredient_category_id = Int32()

    is_main_protein = Bool()
    is_main_carbohydrate = Bool()


async def recipe_main_ingredient_category(request: RetrivalRequest) -> pl.LazyFrame:
    df = await MainIngredients.query().all().join(
        IngredientCategories.query().all(),
        method="inner",
        left_on="ingredient_category_id",
        right_on="ingredient_category_id"
    ).to_lazy_polars()


    recipes = df.select("recipe_id").unique()

    recipes = recipes.join(
        df.filter(pl.col("is_main_protein")).select([
            pl.col("recipe_id"),
            pl.col("root_category_id").alias("main_protein_category_id"),
            pl.col("root_category_name").alias("main_protein_name"),
        ]),
        on="recipe_id",
        how="left"
    )
    recipes = recipes.join(
        df.filter(pl.col("is_main_carbohydrate")).select([
            pl.col("recipe_id"),
            pl.col("root_category_id").alias("main_carbohydrate_category_id"),
            pl.col("root_category_name").alias("main_carboydrate_name"),
        ]),
        on="recipe_id",
        how="left"
    )

    return recipes


def is_non_of(features: list[str]) -> pl.Expr:
    expr = pl.lit(False)

    for feature in features:
        expr = expr | pl.col(feature)

    return expr.not_()


@feature_view(
    name="recipe_main_ingredient_category",
    source=CustomMethodDataSource.from_load(
        recipe_main_ingredient_category,
        depends_on={
            MainIngredients.location,
            IngredientCategories.location
        }
    ),
    materialized_source=materialized_data.parquet_at("recipe_main_ingredient_category.parquet")
)
class RecipeMainIngredientCategory:
    recipe_id = Int32().as_entity()

    main_protein_category_id = Int32().is_optional()
    main_carbohydrate_category_id = Int32().is_optional()
    main_carboydrate_name = String().is_optional()
    main_protein_name = String().is_optional()


    is_salmon = main_protein_category_id == 1216  # noqa: PLR2004
    is_cod = main_protein_category_id == 1236  # noqa: PLR2004
    is_pork = main_protein_category_id == 1070  # noqa: PLR2004
    is_chicken = main_protein_category_id == 1503  # noqa: PLR2004
    is_beef = main_protein_category_id == 1128  # noqa: PLR2004
    is_lamb = main_protein_category_id == 1099  # noqa: PLR2004
    is_shrimp = main_protein_category_id == 1445  # noqa: PLR2004
    is_mixed_meat = main_protein_category_id == 1186  # noqa: PLR2004
    is_tuna = main_protein_category_id == 1388  # noqa: PLR2004

    is_seefood = Bool().transformed_using_features_polars(
        [is_salmon, is_cod, is_tuna, is_shrimp],
        pl.col("is_salmon") | pl.col("is_cod") | pl.col("is_tuna") | pl.col("is_shrimp") # type: ignore
    )

    all_proteins = [is_salmon, is_cod, is_pork, is_chicken, is_beef, is_lamb, is_shrimp, is_mixed_meat, is_tuna]
    is_other_protein = Bool().transformed_using_features_polars(
        using_features=all_proteins, # type: ignore
        transformation=is_non_of([
            "is_salmon",
            "is_cod",
            "is_pork",
            "is_chicken",
            "is_beef",
            "is_lamb",
            "is_shrimp",
            "is_mixed_meat",
            "is_tuna"
        ]) # type: ignore
    )

    is_grain = main_carbohydrate_category_id == 1047  # noqa: PLR2004
    is_soft_bread = main_carbohydrate_category_id == 1699  # noqa: PLR2004
    is_vegetables = main_carbohydrate_category_id == 940  # noqa: PLR2004
    is_pasta = main_carbohydrate_category_id == 2182  # noqa: PLR2004

    all_carbos = [is_grain, is_soft_bread, is_vegetables, is_pasta]
    is_other_carbo = Bool().transformed_using_features_polars(
        using_features=all_carbos, # type: ignore
        transformation=is_non_of([
            "is_grain", "is_soft_bread", "is_vegetables", "is_pasta"
        ]) # type: ignore
    )



main_ingredient_ids = {
    "fish": 1,
    "meat": 2,
    "vegetarian": 3,
    "shellfish": 4,
    "poultry": 5,
    "pork": 7,
    "beef": 8,
    "lactose": 11,
    "vegan": 12,
}


@feature_view(
    name="recipe_features",
    source=adb.fetch(recipe_features_sql),
    materialized_source=materialized_data.parquet_at("recipe_features.parquet"),
    acceptable_freshness=timedelta(hours=6),
)
class RecipeFeatures:
    recipe_id = Int32().lower_bound(1).upper_bound(10_000_000).as_entity()

    updated_at = EventTimestamp()
    loaded_at = Timestamp()

    main_recipe_id = Int32()
    main_ingredient_id = Int32()
    company_id = String()

    year = Int32()
    week = Int32()

    recipe_name = String()
    average_rating = Float().is_optional()
    number_of_ratings = Int32().is_optional()

    number_of_ratings_log = (
        number_of_ratings.log1p()
        .is_optional()
        .description("Taking the log in order to get it closer to a normal distribution.")
    )

    cooking_time_from = Int32()
    cooking_time_to = Int32()

    is_low_cooking_time = cooking_time_from <= 15 # noqa: PLR2004
    is_medium_cooking_time = (cooking_time_from == 20).logical_or(cooking_time_from == 25) # noqa: PLR2004
    is_high_cooking_time = cooking_time_from >= 30 # noqa: PLR2004

    taxonomies = List(String())
    taxonomy_ids = List(Int32())

    is_addon_kit = taxonomy_ids.contains(2164)
    is_adams_signature = taxonomy_ids.contains(2146)
    is_weight_watchers = taxonomy_ids.contains(1878)

    is_roede = taxonomy_ids.transform_polars(
        pl.col("taxonomy_ids").list.contains(2015)
        | pl.col("taxonomy_ids").list.contains(2096),
        as_dtype=Bool()
    )

    is_chefs_choice = taxonomy_ids.transform_polars(
        pl.col("taxonomy_ids").list.contains(2011)
        | pl.col("taxonomy_ids").list.contains(2147)
        | pl.col("taxonomy_ids").list.contains(2152),
        as_dtype=Bool()
    ).description("Also known as inspirational in some places")

    is_family_friendly = taxonomy_ids.transform_polars(
        pl.col("taxonomy_ids").list.contains(2148)
        | pl.col("taxonomy_ids").list.contains(2014)
        | pl.col("taxonomy_ids").list.contains(2153),
        as_dtype=Bool()
    )

    is_low_calorie = taxonomy_ids.transform_polars(
        pl.col("taxonomy_ids").list.contains(2156)
        | pl.col("taxonomy_ids").list.contains(2013)
        | pl.col("taxonomy_ids").list.contains(2151),
        as_dtype=Bool()
    )

    is_kids_friendly = (
        taxonomies.contains("Børnevenlig")
        .logical_or(taxonomies.contains("Barnevennlig"))
        .description("Not ideal solution, but works for now")
    )

    is_lactose = (
        taxonomies.contains("Laktosefri")
        .logical_or(taxonomies.contains("Laktosfri"))
        .description("Not ideal solution, but works for now")
    )

    is_spicy = taxonomies.contains("Stark/Spicy").logical_or(taxonomies.contains("Stærk/krydret"))
    is_gluten_free = taxonomies.contains("Glutenfri")

    (
        is_vegetarian_ingredient,
        is_vegan,
        is_fish
    ) = main_ingredient_id.one_hot_encode(
        [  # type: ignore
            main_ingredient_ids["vegetarian"],
            main_ingredient_ids["vegan"],
            main_ingredient_ids["fish"],
        ]
    )
    is_vegetarian = is_vegetarian_ingredient.logical_or(is_vegan)


@feature_view(
    name="main_recipe_features",
    source=RecipeFeatures.metadata.materialized_source.transform_with_polars( # type: ignore
        lambda df: df.filter(
            pl.col("year") * 100 + pl.col("week") >= 202430 # noqa: PLR2004
        ).sort("recipe_id").unique(
            "main_recipe_id", keep="first", maintain_order=True
        )
    )
)
class MainRecipeFeature:
    """
    Mainly used to filter out data for computational heavy stuff.
    Like the recipe embeddings.
    """

    main_recipe_id = Int32().as_entity()
    recipe_id = Int32()
    company_id = String()

    year = Int32()
    week = Int32()

    recipe_name = String()


@model_contract(
    name="recipe_embedding",
    input_features=[MainRecipeFeature().recipe_name],
    exposed_model=openai_embedding("text-embedding-3-small", batch_on_n_chunks=100),
    output_source=materialized_data.partitioned_parquet_at(
        "recipe_embeddings",
        partition_keys=["company_id"]
    ),
    acceptable_freshness=timedelta(hours=6)
)
class RecipeEmbedding:
    main_recipe_id = Int32().as_entity()
    company_id = String().as_entity()
    recipe_id = Int32()
    recipe_name = String()
    embedding = Embedding(1536)
    predicted_at = EventTimestamp()


@feature_view(
    name="mealkit_recipe_similarity",
    source=InMemorySource.empty(),
    description="Computes the mealkit recipe similarity given a mealkit embedding."
)
class MealkitRecipeSimilarity:
    mealkit_embedding = Embedding(1536).description(
        "Assumes that it is normalized to 1 so we can use the dot product"
    )
    similarity = mealkit_embedding.dot_product(
        RecipeEmbedding().embedding
    )


@feature_view(
    name="recipe_taxonomies",
    description="The taxonomies associated with a recipe.",
    materialized_source=materialized_data.parquet_at("recipe_taxonomies.parquet"),
    source=adb_ml.fetch(taxonomies_sql),
    acceptable_freshness=timedelta(days=6),
    contacts=contacts,
)
class RecipeTaxonomies:
    recipe_id = Int32().as_entity()

    loaded_at = EventTimestamp()

    recipe_taxonomies = String().description(
        "All the taxonomies seperated by a ',' char.",
    )


@feature_view(
    name="recipe_ingredients",
    source=adb_ml.fetch(recipe_ingredients_sql),
    materialized_source=materialized_data.parquet_at("recipe_ingredients.parquet"),
    description="All non base ingredients that a recipe contains.",
    acceptable_freshness=timedelta(days=6),
    contacts=contacts,
)
class RecipeIngredient:
    recipe_id = Int32().as_entity()

    loaded_at = EventTimestamp()

    all_ingredients = String().description(
        "All the ingredients seperated by a ',' char.",
    )

    contains_salmon = all_ingredients.contains("laks")
    contains_chicken = all_ingredients.contains("kylling")
    contains_meat = all_ingredients.contains("kjøtt")


@feature_view(
    name="recipe_nutrition",
    source=adb.with_schema("pim").table("RECIPE_NUTRITION_FACTS").with_loaded_at(),
    materialized_source=materialized_data.parquet_at("recipe_nutrition.parquet"),
    acceptable_freshness=timedelta(days=4),
)
class RecipeNutrition:
    recipe_id = Int32().lower_bound(1).as_entity()
    portion_size = Int32().lower_bound(1).upper_bound(6).as_entity()

    loaded_at = EventTimestamp()

    energy_kcal_per_portion = Float().lower_bound(0).is_optional()
    carbs_pct = Float().lower_bound(0).is_optional()
    fat_pct = Float().lower_bound(0).is_optional()
    fat_saturated_pct = Float().lower_bound(0).is_optional()
    protein_pct = Float().lower_bound(0).is_optional()
    fruit_veg_fresh_p = Float().lower_bound(0).is_optional()


@feature_view(
    name="recipe_cost",
    source=adb.with_schema("mb")
    .table(
        "recipe_costs_pim",
        mapping_keys={
            "PORTION_SIZE": "portion_size",
            "PORTION_ID": "portion_id",
            "recipe_cost_whole_units_pim": "recipe_cost_whole_units",
            "recipes_year": "menu_year",
            "recipes_week": "menu_week",
        },
    )
    .with_loaded_at(),
    materialized_source=materialized_data.parquet_at("recipe_cost"),
    acceptable_freshness=timedelta(days=6),
)
class RecipeCost:
    recipe_id = Int32().lower_bound(1).as_entity()
    portion_size = Int32().lower_bound(1).upper_bound(6).as_entity()

    portion_id = Int32().lower_bound(1).upper_bound(30)

    menu_year = Int32().lower_bound(2023).upper_bound(2050)
    menu_week = Int32().lower_bound(1).upper_bound(53)

    loaded_at = EventTimestamp()

    country = String()
    company_name = String()

    main_recipe_id = Int32().lower_bound(1)
    recipe_name = String()
    portions = String().description(
        "Needs to be a string because we have instances of '2+' in Danmark.",
    )
    is_plus_portion = portions.contains("\\+")

    recipe_cost_whole_units = Float().description(
        "Also known as the Cost of Food. The planed summed ingredient cost"
    )

    price_category_max_price = Int32()
    price_category_level = Int32()

    is_premium = price_category_level >= 4  # noqa: PLR2004
    is_cheep = price_category_level <= -1

    suggested_selling_price_incl_vat = Float()


def compute_recipe_features(store: ContractStore | None = None) -> RetrivalJob:

    def query(view_wrapper: FeatureViewWrapper) -> FeatureViewStore:
        """
        Makes it easier to swap between prod, and manually defined data for testing.
        """
        if store:
            return store.feature_view(view_wrapper)
        else:
            return view_wrapper.query()

    nutrition = query(RecipeNutrition).all()
    cost = query(RecipeCost).select_columns([
        "recipe_cost_whole_units", "is_plus_portion", "is_cheep"
    ]).transform_polars(lambda df: df.filter(pl.col("is_plus_portion").not_()))

    return (
        query(RecipeFeatures).all()
        .transform_polars(lambda df: df.filter(pl.col("is_addon_kit").not_()))
        .join(nutrition, method="inner", left_on="recipe_id", right_on="recipe_id")
        .with_request(nutrition.retrival_requests)  # Hack to get around a join bug
        .join(
            cost,
            method="inner",
            left_on=["recipe_id", "portion_size"],
            right_on=["recipe_id", "portion_size"],
        )
        .rename({"recipe_cost_whole_units": "cost_of_food"})
    )


async def compute_normalized_features(
    request: RetrivalRequest, limit: int | None, store: ContractStore | None = None
) -> pl.LazyFrame:

    menu_recipe_features = await compute_recipe_features(store).derive_features([request]).to_polars()

    needed_features = [(feature.name, feature.dtype) for feature in request.returned_features.union(request.entities)]

    min_max_scaled_features = [name for name, dtype in needed_features if "float" in dtype.name and dtype.is_numeric]
    other_features = [name for name, _ in needed_features if name not in min_max_scaled_features]

    menu_recipe_features = menu_recipe_features.with_columns(year_week=pl.col("year") * 100 + pl.col("week"))

    results: pl.DataFrame | None = None
    for yearweek, portion_size, company_id in (
        menu_recipe_features[["year_week", "portion_size", "company_id"]].unique().iter_rows()
    ):
        features = menu_recipe_features.filter(
            (pl.col("year_week") == yearweek)
            & (pl.col("portion_size") == portion_size)
            & (pl.col("company_id") == company_id)
        )

        max_value = features.select(min_max_scaled_features).fill_null(0).fill_nan(0).max().transpose().to_series()
        min_value = (
            features.select(min_max_scaled_features).fill_null(10000).fill_nan(10000).min().transpose().to_series()
        )

        normalized = (features.select(min_max_scaled_features).transpose() - min_value) / (max_value - min_value)
        normalized = normalized.transpose().rename(
            lambda x: min_max_scaled_features[int(x.split("_")[1])],
        )
        normalized = pl.concat(
            [
                normalized,
                features.select(other_features),
            ],
            how="horizontal",
        )
        normalized = normalized.fill_null(0).fill_nan(0)

        if results is None:
            results = normalized
        else:
            results = results.vstack(normalized.select(results.columns))

    assert results is not None
    return results.lazy()


@feature_view(
    name="normalized_recipe_features",
    description="Contains normalized features based on the year week menu.",
    source=CustomMethodDataSource.from_methods(
        all_data=compute_normalized_features,
        depends_on_sources={
            RecipeFeatures.location,
            RecipeNutrition.location,
            RecipeMainIngredientCategory.location,
            RecipeCost.location
        },
    ).with_loaded_at(),
    materialized_source=materialized_data.partitioned_parquet_at(
        "normalized_recipe_features_per_company", partition_keys=["company_id", "year"]
    ),
    acceptable_freshness=timedelta(days=4),
)
class NormalizedRecipeFeatures:
    recipe_id = Int32().as_entity()
    portion_size = Int32().as_entity()
    company_id = String().as_entity()

    normalized_at = EventTimestamp()

    main_recipe_id = Int32()

    taxonomy_ids = List(Int32())

    # PoC. Should rather only use a subset of the taxonomy types
    taxonomy_of_interest = taxonomy_ids.transform_polars(
        pl.col("taxonomy_ids").list.unique().list.set_difference(
            pl.lit([
                971, 1837, 985, 1838, 1178, 1212, 986, 1064, 1213, 1839, 991, 184, 247, 2096, 226, 217, 1177, 1067
            ])
        ),
        as_dtype=List(Int32())
    )

    year = Int32()
    week = Int32()

    average_rating = Float().is_optional()
    cost_of_food = Float()

    number_of_ratings_log = Float().description("Taking the log in order to get it closer to a normal distribution.")

    cooking_time_from = Float()

    is_low_cooking_time = Bool()
    is_medium_cooking_time = Bool()
    is_high_cooking_time = Bool()

    is_family_friendly = Bool()
    is_kids_friendly = Bool()
    is_lactose = Bool()
    is_spicy = Bool()
    is_gluten_free = Bool()

    is_vegan = Bool()
    is_vegetarian = Bool()
    is_fish = Bool()

    is_weight_watchers = Bool()
    is_roede = Bool()
    is_chefs_choice = Bool()
    is_low_calorie = Bool()

    is_adams_signature = Bool()
    is_cheep = Bool()

    energy_kcal_per_portion = Float()
    carbs_pct = Float()
    fat_pct = Float()
    fat_saturated_pct = Float()
    protein_pct = Float()
    fruit_veg_fresh_p = Float()


ingredient_allergy_preferences_sql = """
SELECT *
FROM (
    SELECT
        atr.allergy_name,
        atr.allergy_id,
        ia.has_trace_of,
        ap.preference_id,
        i.ingredient_id,
        ROW_NUMBER() over (
            PARTITION BY atr.allergy_id, i.ingredient_id, ap.preference_id ORDER BY atr.LANGUAGE_ID
        ) as nr
    FROM
        ingredients i
    INNER JOIN
        ingredient_allergies ia ON ia.ingredient_id = i.ingredient_id
    INNER JOIN
        allergies_translations atr ON atr.allergy_id = ia.allergy_id
    LEFT JOIN
        allergies_preference ap ON ap.allergy_id = atr.allergy_id
) as allergies
WHERE allergies.nr = 1
        """

@feature_view(
    name="ingredient_allergy_preferences",
    source=pim_core.fetch(ingredient_allergy_preferences_sql),
    materialized_source=materialized_data.parquet_at("ingredient_allergy_preferences.parquet")
)
class IngredientAllergiesPreferences:
    ingredient_id = Int32().as_entity()
    allergy_id = Int32().as_entity()

    has_trace_of = Bool().description("If the ingredient only have traces of the allergy")

    preference_id = String().is_optional()
    allergy_name = String()


all_recipe_ingredients_sql = """
SELECT *
FROM (
    SELECT
        rp.RECIPE_ID as recipe_id,
        it.INGREDIENT_NAME as ingredient_name,
        i.created_date as created_at,
        p.PORTION_SIZE as portion_size,
        p.PORTION_ID as portion_id,
        s.supplier_name,
        i.ingredient_id,
        ROW_NUMBER() over (PARTITION BY rp.recipe_id, i.ingredient_id, rp.PORTION_ID ORDER BY it.LANGUAGE_ID) as nr
    FROM pim.RECIPE_PORTIONS rp
    INNER JOIN pim.PORTIONS p on p.PORTION_ID = rp.PORTION_ID
    INNER JOIN pim.CHEF_INGREDIENT_SECTIONS cis ON cis.recipe_portion_id = rp.recipe_portion_id
    INNER JOIN pim.chef_ingredients ci ON ci.chef_ingredient_section_id = cis.chef_ingredient_section_id
    INNER JOIN pim.order_ingredients oi ON oi.order_ingredient_id = ci.order_ingredient_id
    INNER JOIN pim.ingredients i on i.ingredient_internal_reference = oi.INGREDIENT_INTERNAL_REFERENCE
    INNER JOIN pim.INGREDIENTS_TRANSLATIONS it ON it.INGREDIENT_ID = i.ingredient_id
    INNER JOIN pim.suppliers s ON s.supplier_id = i.supplier_id
) as ingredients
WHERE ingredients.nr = 1
"""

@feature_view(
    name="all_recipe_ingredients",
    source=adb.fetch(all_recipe_ingredients_sql),
    materialized_source=materialized_data.parquet_at("all_recipe_ingredients.parquet")
)
class AllRecipeIngredients:
    recipe_id = Int32().as_entity()
    ingredient_id = Int32().as_entity()
    portion_id = Int32().as_entity()

    created_at = EventTimestamp()

    portion_size = Int32()

    ingredient_name = String()
    supplier_name = String()

    is_house_hold_ingredient = supplier_name == "Basis"


recipe_preferences_sql = """WITH distinct_recipe_preferences AS (
    SELECT DISTINCT
        rp.PORTION_ID,
        pt.PORTION_SIZE as portion_size,
        r.recipe_id,
        r.main_recipe_id,
        rmt.recipe_name,
        p.preference_id,
        p.name,
        menu_year,
        menu_week
    FROM pim.weekly_menus wm
    INNER JOIN pim.menus m ON m.weekly_menus_id = wm.weekly_menus_id
        AND m.product_type_id in (
            'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1',
            '2F163D69-8AC1-6E0C-8793-FF0000804EB3'
        ) --Velg&Vrak dishes
    INNER JOIN pim.menu_variations mv ON mv.menu_id = m.menu_id
    INNER JOIN pim.menu_recipes mr ON mr.MENU_ID = m.menu_id
        AND mr.MENU_RECIPE_ORDER <= mv.MENU_NUMBER_DAYS
    INNER JOIN pim.recipes r ON r.recipe_id = mr.RECIPE_ID
    INNER JOIN cms.company c ON c.id = wm.company_id
    INNER JOIN cms.country cont ON cont.id = c.country_id
    INNER JOIN pim.recipe_metadata_translations rmt ON rmt.recipe_metadata_id = r.recipe_metadata_id
        AND rmt.language_id = cont.default_language_id
    INNER JOIN pim.recipe_portions rp ON rp.RECIPE_ID = r.recipe_id AND rp.portion_id = mv.PORTION_ID
    INNER JOIN pim.portions pt ON pt.PORTION_ID = rp.PORTION_ID
    INNER JOIN pim.chef_ingredient_sections cis ON cis.RECIPE_PORTION_ID = rp.recipe_portion_id
    INNER JOIN pim.chef_ingredients ci ON ci.CHEF_INGREDIENT_SECTION_ID = cis.CHEF_INGREDIENT_SECTION_ID
    INNER JOIN pim.order_ingredients oi ON oi.ORDER_INGREDIENT_ID = ci.ORDER_INGREDIENT_ID
    INNER JOIN pim.ingredients i ON i.ingredient_internal_reference = oi.INGREDIENT_INTERNAL_REFERENCE
    INNER JOIN pim.find_ingredient_categories_parents icc ON icc.ingredient_id = i.ingredient_id
    INNER JOIN pim.ingredient_category_preference icp ON icp.ingredient_category_id = icc.parent_category_id
    INNER JOIN cms.preference p ON p.preference_id = icp.preference_id
        AND p.preference_type_id = '4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB'
    INNER JOIN cms.preference_company pc ON pc.company_id = wm.company_id
        AND pc.preference_id = p.preference_id AND pc.is_active = '1'
    WHERE m.RECIPE_STATE = 1
)

SELECT
    PORTION_ID as portion_id,
    portion_size,
    menu_year,
    menu_week,
    recipe_id,
    main_recipe_id,
    CONCAT('["', CONCAT(STRING_AGG(convert(nvarchar(36), preference_id), '", "'), '"]')) as preference_ids,
    CONCAT('["', CONCAT(STRING_AGG(name, '", "'), '"]')) as preferences
FROM distinct_recipe_preferences
GROUP BY PORTION_ID, portion_size, menu_year, menu_week, recipe_id, main_recipe_id
"""


@feature_view(
    name="recipe_preferences",
    source=adb.fetch(recipe_preferences_sql).with_loaded_at(),
    materialized_source=materialized_data.parquet_at("recipe_preferences.parquet"),
    acceptable_freshness=timedelta(days=5),
)
class RecipePreferences:
    recipe_id = Int32().as_entity()
    portion_size = Int32().as_entity()

    loaded_at = EventTimestamp()

    portion_id = Int32()

    menu_year = Int32()
    menu_week = Int32()

    main_recipe_id = Int32()

    preference_ids = List(String())
    preferences = List(String())


async def join_recipe_and_allergies(
    request: RetrivalRequest, store: ContractStore | None = None
) -> pl.LazyFrame:

    def query(view_wrapper: FeatureViewWrapper) -> FeatureViewStore:
        """
        Makes it easier to swap between prod, and manually defined data for testing.
        """
        if store:
            return store.feature_view(view_wrapper)
        else:
            return view_wrapper.query()


    allergy_preferences = await query(IngredientAllergiesPreferences).filter(
        pl.col("preference_id").is_not_null()
    ).to_polars()

    recipes_with_allergies = await query(AllRecipeIngredients).filter(
        pl.col("ingredient_id").is_in(allergy_preferences["ingredient_id"])
    ).to_lazy_polars()

    recipe_with_preferences = recipes_with_allergies.join(
        allergy_preferences.lazy(),
        on="ingredient_id"
    ).group_by(["recipe_id", "portion_id", "portion_size"]).agg(
        allergy_preference_ids=pl.col("preference_id")
    )
    recipe_preferences = await query(RecipePreferences).all().to_lazy_polars()

    all_preferences = recipe_with_preferences.cast({"portion_id": pl.Int32}).join(
        recipe_preferences.cast({"portion_id": pl.Int32}),
        on=["recipe_id", "portion_id"],
        how="full"
    ).with_columns(
        preference_ids=pl.col("preference_ids").fill_null(["870C7CEA-9D06-4F3E-9C9B-C2C395F5E4F5"]).list.concat(
            pl.col("allergy_preference_ids").fill_null([])
        ).list.unique(),
        recipe_id=pl.col("recipe_id").fill_null(pl.col("recipe_id_right")),
        portion_size=pl.col("portion_size").fill_null(pl.col("portion_size_right")),
        portion_id=pl.col("portion_id").fill_null(pl.col("portion_id_right"))
    )
    return all_preferences


@feature_view(
    name="recipe_negative_preferances",
    source=CustomMethodDataSource.from_load(
        join_recipe_and_allergies,
        depends_on={
            RecipePreferences.location,
            IngredientAllergiesPreferences.location,
            AllRecipeIngredients.location,
        }
    ),
    materialized_source=materialized_data.parquet_at("recipe_negative_preferences.parquet"),
    acceptable_freshness=timedelta(days=5)
)
class RecipeNegativePreferences:
    recipe_id = Int32().as_entity()
    portion_size = Int32().as_entity()

    loaded_at = EventTimestamp()

    portion_id = String()

    preference_ids = List(String())
