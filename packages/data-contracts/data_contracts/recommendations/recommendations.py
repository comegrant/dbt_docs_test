from aligned import EventTimestamp, Int32, Json, String, model_contract
from data_contracts.orders import HistoricalRecipeOrders, MealboxChangesAsRating
from data_contracts.recommendations.recipe_clustering import RecipeCluster
from data_contracts.recommendations.user_recipe_likability import (
    UserRecipeLikability,
)
from data_contracts.sources import (
    adb_ml,
    adb_ml_output,
    recommendations_dir,
    segment_personas_db,
)
from project_owners.owner import Owner

likability = UserRecipeLikability()
cluster = RecipeCluster()

rec_contacts = [Owner.niladri().name, Owner.matsmoll().name]


delivered_recipes = HistoricalRecipeOrders()
behavioral_ratings = MealboxChangesAsRating()


@model_contract(
    name="rec_engine",
    contacts=rec_contacts,
    description="The ranking of recipes per user, within a given week menu.",
    input_features=[likability.score],
    output_source=recommendations_dir.partitioned_parquet_at(
        "partitioned_recommendations", partition_keys=["company_id", "year", "week"]
    ),
    application_source=adb_ml_output.table(
        "latest_recommendations",
        mapping_keys={"run_timestamp": "predicted_at"},
    ),
)
class RecommendatedDish:
    agreement_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()
    product_id = (
        String()
        .as_entity()
        .description(
            "The external menu ID for the recipe. "
            "This is what the frontend uses to identify the recipe. "
            "For a given week, and portion I think.",
        )
    )
    predicted_at = EventTimestamp()
    company_id = String()

    order_rank = (
        Int32().lower_bound(1).description("The rank of the recipe in the recommendation. 1 is the best.")
        # .as_recommendation_target()
        # .estemating_rank(behavioral_ratings.rating)
    )


rec_engine = RecommendatedDish()


@model_contract(
    name="presented_recommendations",
    contacts=rec_contacts,
    description="The top n recommendations in the format that frontend expects.",
    input_features=[
        rec_engine.year,
        rec_engine.week,
        rec_engine.product_id,
        rec_engine.order_rank,
    ],
    output_source=adb_ml.with_schema("personas").table("recommendations"),
    application_source=segment_personas_db.table("recommendations"),
)
class PresentedRecommendations:
    agreement_id = Int32().as_entity()

    company_id = String()
    recommendation_json = Json()
