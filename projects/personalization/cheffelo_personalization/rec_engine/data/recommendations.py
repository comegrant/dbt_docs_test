from aligned import EventTimestamp, Float, Int32, Json, String, model_contract

from cheffelo_personalization.rec_engine.contacts import Contacts
from cheffelo_personalization.rec_engine.data.recipe_clustering import RecipeCluster
from cheffelo_personalization.rec_engine.data.recipe import HistoricalRecipeOrders
from cheffelo_personalization.rec_engine.data.user_recipe_likability import (
    UserRecipeLikability,
)
from cheffelo_personalization.rec_engine.sources import (
    adb_ml,
    adb_ml_output,
    model_preds,
    segment_personas_db,
)

likability = UserRecipeLikability()
cluster = RecipeCluster()

rec_contacts = [
    Contacts.niladri().markdown(),
    Contacts.jose().markdown(),
    Contacts.matsmoll().markdown(),
]

delivered_recipes = HistoricalRecipeOrders()

@model_contract(
    name="rec_engine",
    contacts=rec_contacts,
    description="The ranking of recipes per user, within a given week menu.",
    features=[likability.score, cluster.cluster],
    prediction_source=model_preds.parquet_at("recommendation_products.parquet"),
    application_source=adb_ml_output.table(
        "latest_recommendations", mapping_keys={"run_timestamp": "predicted_at"}
    ),
)
class RecommendatedDish:
    agreement_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()
    product_id = String().as_entity().description("The external menu ID for the recipe")

    predicted_at = EventTimestamp()

    company_id = String()

    order_of_relevance_cluster = (Int32()
        .lower_bound(1)
        # .as_recommendation_target()
        # .as_recommendation_ranking()
        # .binary_target(delivered_recipes)
        # .scalar_target(delivered_recipes.rating)
    )


@model_contract(
    name="backup_recommendations",
    contacts=rec_contacts,
    description="The recommendation used when we have no data on the user.",
    features=[likability.score],
    prediction_source=model_preds.parquet_at("backup_recommendations.parquet"),
)
class BackupRecommendations:
    recipe_id = String().as_entity()
    predicted_at = EventTimestamp()
    score = Float()


rec_engine = RecommendatedDish()


@model_contract(
    name="presented_recommendations",
    contacts=rec_contacts,
    description="The top n recommendations in the format that frontend expects.",
    features=[
        rec_engine.year,
        rec_engine.week,
        rec_engine.product_id,
        rec_engine.order_of_relevance_cluster,
    ],
    prediction_source=adb_ml.with_schema("personas").table("recommendations"),
    application_source=segment_personas_db.table("recommendations"),
)
class PresentedRecommendations:
    agreement_id = Int32().as_entity()

    company_id = String()
    recommendation_json = Json()
    # run_timestamp = EventTimestamp()
