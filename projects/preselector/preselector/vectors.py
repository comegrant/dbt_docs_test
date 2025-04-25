import logging
from typing import Annotated

import polars as pl
from aligned import (
    ContractStore,
)
from data_contracts.preselector.basket_features import (
    BasketFeatures,
    ImportanceVector,
    PredefinedVectors,
    TargetVectors,
    VariationTags,
)

from preselector.monitor import duration
from preselector.schemas.batch_request import GenerateMealkitRequest

logger = logging.getLogger(__name__)


async def importance_vector_for_concept(
    concept_ids: list[str], store: ContractStore, company_id: str
) -> tuple[pl.DataFrame, pl.DataFrame]:
    features = BasketFeatures.query().request.request_result.feature_columns

    entities: dict[str, list] = {"concept_id": [], "company_id": [], "vector_type": []}

    for concept_id in concept_ids:
        entities["concept_id"].extend([concept_id, concept_id])
        entities["company_id"].extend([company_id, company_id])
        entities["vector_type"].extend(["importance", "target"])

    predefined_vector = await store.feature_view(PredefinedVectors).features_for(entities).drop_invalid().to_polars()

    combined_feature = [
        pl.col(feature).where(pl.col(feature) > 0).mean().fill_nan(0).fill_null(0) for feature in features
    ]
    combined_targets: list[pl.DataFrame] = []

    importances = predefined_vector.filter(pl.col("vector_type") == "importance").with_columns(
        pl.col(feat) / pl.sum_horizontal(features) for feat in features
    )

    # Setting the new target based on the following formula when there are multiple concepts
    # target_f = (target_1 * importance_1 + target_2 * importance_2) / sum(importance_f)
    for concept_id in concept_ids:
        importance = importances.filter(pl.col("concept_id") == concept_id).select(features)

        target = predefined_vector.filter(
            (pl.col("vector_type") == "target") & (pl.col("concept_id") == concept_id)
        ).select(features)

        combined_targets.append((importance.transpose() * target.transpose()).transpose())

    if not combined_targets:
        raise ValueError(
            f"Unable to find any target or importance vector for the concept ids: {concept_ids} in company {company_id}"
        )

    target_vector = (
        ((pl.concat(combined_targets).sum().transpose() / importances.select(features).sum().transpose()).fill_nan(0))
        .transpose()
        .rename(lambda col: features[int(col.split("_")[1])])
    )

    importance_vector = importances.select(combined_feature)

    assert (
        not importance_vector.is_empty()
    ), f"Predefined importance vector is missing for concept {concept_ids} company: {company_id}"
    assert (
        not target_vector.is_empty()
    ), f"Predefined target vector is missing for concept {concept_ids} company: {company_id}"
    return (importance_vector, target_vector)


def potentially_add_variation(importance: pl.DataFrame, target: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    features = list(BasketFeatures.compile().request_all.needed_requests[0].all_features)

    def contains_features(features: list[str], importance: pl.DataFrame) -> bool:
        if not features:
            return True
        max_imp_value = importance.select([pl.col(feature) for feature in features]).max_horizontal().to_list()[0]
        return max_imp_value != 0

    def fill_when_missing(
        default_values: dict[str, float], importance: pl.DataFrame, target: pl.DataFrame, fixed_importance: float
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        importance = importance.with_columns([pl.lit(fixed_importance).alias(key) for key in default_values])
        target = target.with_columns([pl.lit(val).alias(key) for key, val in default_values.items()])

        return importance, target

    potential_tags = {
        VariationTags.protein: 0.1,
        VariationTags.carbohydrate: 0.2,
        VariationTags.quality: 0.8,
        VariationTags.equal_dishes: 0.0,
    }

    tags = dict()

    for tag, value in potential_tags.items():
        feat = [feature.name for feature in features if feature.tags and tag in feature.tags]

        if not contains_features(feat, importance):
            tags[tag] = value

    if not tags:
        return importance, target

    # These total sum 0.5 will lead to
    # Attributes = 2/3
    # Variation = 1/3
    # Since they should be summed to 1
    total_sum = 0.2 / len(tags)

    if not tags:
        return importance, target

    for tag, value in tags.items():
        vector = {feature.name: value for feature in features if feature.tags and tag in feature.tags}

        importance, target = fill_when_missing(vector, importance, target, total_sum / len(vector))

    return importance, target


def handle_calorie_concept(
    target: pl.DataFrame, importance: pl.DataFrame, concept_ids: list[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Changes the importance and target vector in case it is either Roede, og low calorie.

    These are polarised, so it changes the vectors quite a lot.
    """
    low_cal_id = "FD661CAD-7F45-4D02-A36E-12720D5C16CA"
    vegetarian_id = "6A494593-2931-4269-80EE-470D38F04796"
    roede_id = "DF81FF77-B4C4-4FC1-A135-AB7B0704D1FA"

    # Roede
    if concept_ids == [low_cal_id]:
        return (target.with_columns(is_low_calorie=pl.lit(1)), importance.with_columns(is_low_calorie=pl.lit(1)))
    elif roede_id in concept_ids:
        # Roede can choose negative preferences, so we will not select a pre-defined mealkit
        # But rather find the most optimal selection
        # As a result will we weight the roede features above everything else.
        # But if there are no left, then they will start to get other types of dishes
        target = target.with_columns(is_roede_percentage=pl.lit(1))
        importance = importance.with_columns(is_roede_percentage=pl.lit(1))
        return target, importance
    else:
        target = target.with_columns(is_roede_percentage=pl.lit(0))
        importance = importance.with_columns(is_roede_percentage=pl.lit(1))

    if vegetarian_id not in concept_ids and low_cal_id not in concept_ids:
        return (target.with_columns(is_low_calorie=pl.lit(0)), importance.with_columns(is_low_calorie=pl.lit(0.5)))
    else:
        return target, importance


async def historical_preselector_vector(
    agreement_id: int,
    request: GenerateMealkitRequest,
    store: ContractStore,
) -> tuple[pl.DataFrame, pl.DataFrame, Annotated[bool, "If the vectors is based on historical data"]]:
    """
    Loads the importance and target vectors for a customer.
    """

    vector_features = [
        feat.name for feat in store.feature_view(TargetVectors).request.features if "float" in feat.dtype.name
    ]

    async def inject_importance_and_target(
        importance: pl.DataFrame, target: pl.DataFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        from data_contracts.preselector.basket_features import InjectedFeatures

        weighting = 0.65

        importance_static = (
            await InjectedFeatures.process_input(
                {
                    "mean_cost_of_food": [0.03],
                    "mean_rank": [0.05],
                    "mean_ordered_ago": [0.4],
                    "intra_week_similarity": [0.09],
                    "repeated_proteins_percentage": [0.1],
                    "repeated_carbo_percentage": [0.05],
                    "mean_is_dislike": [0.1],
                    "mean_is_favorite": [0.1],
                }
            )
            .drop_invalid()
            .to_polars()
        ).to_dicts()[0]

        target_static = (
            await InjectedFeatures.process_input(
                {
                    # Mean cost target will be set in the generate week
                    "mean_cost_of_food": [0],
                    "mean_rank": [0],
                    # aka max
                    "mean_ordered_ago": [0],
                    "intra_week_similarity": [0],
                    "repeated_proteins_percentage": [0],
                    "repeated_carbo_percentage": [0],
                    "mean_is_dislike": [0],
                    "mean_is_favorite": [1],
                }
            )
            .drop_invalid()
            .to_polars()
        ).to_dicts()[0]

        other_features = [feat for feat in vector_features if feat not in importance_static]

        static_vector = (
            pl.DataFrame(
                dict(
                    **importance_static,
                    **{key: 0 for key in other_features},
                ),
                schema_overrides=importance.schema,
            )
            .select(pl.all() / pl.sum_horizontal(vector_features) * weighting)
            .select(importance.columns)
        )

        merged_importance = (
            pl.concat([importance.select(pl.all() * (1 - weighting)), static_vector], how="vertical_relaxed")
            .sum()
            .select(pl.all() / pl.sum_horizontal(vector_features))
        )

        importance, target = (
            merged_importance,
            target.with_columns([pl.lit(value).alias(key) for key, value in target_static.items()]),
        )

        return (
            importance.with_columns([pl.col(feat) / pl.sum_horizontal(vector_features) for feat in importance.columns]),
            target,
        )

    logger.debug(f"No history found, using default values {request.concept_preference_ids}")

    company_id = request.company_id
    concept_ids = [concept_id.upper() for concept_id in request.concept_preference_ids]

    with duration("load-concept-definitions"):
        default_importance, default_target = await importance_vector_for_concept(concept_ids, store, company_id)

    default_importance = default_importance.with_columns(
        pl.col(feat) / pl.sum_horizontal(vector_features) for feat in vector_features
    )

    if request.has_data_processing_consent:
        with duration("load-importance-vector"):
            user_importance = (
                await store.feature_view(ImportanceVector)
                .features_for(
                    {
                        "agreement_id": [agreement_id],
                    }
                )
                .drop_invalid()
                .to_polars()
            )
    else:
        user_importance = pl.DataFrame()

    if user_importance.is_empty():
        default_importance, default_target = potentially_add_variation(default_importance, default_target)
        default_importance = default_importance.with_columns(
            pl.col(feat) / pl.sum_horizontal(vector_features) for feat in vector_features
        )

        default_target, default_importance = handle_calorie_concept(default_target, default_importance, concept_ids)
        default_importance, default_target = await inject_importance_and_target(
            importance=default_importance, target=default_target
        )
        return default_target, default_importance, False

    with duration("load-target-vector"):
        user_target = (
            await store.feature_view(TargetVectors)
            .features_for(
                {
                    "agreement_id": [agreement_id],
                }
            )
            .drop_invalid()
            .to_polars()
        )

    # Stacking the user vector two times to weight that 2 / 3
    user_vector_weight = 2
    attribute_vector_weight = 1
    vector_sum = user_vector_weight + attribute_vector_weight

    with duration("find-attributes-to-overwrite-in-vector"):
        overwrite_columns: list[str] = [
            key for key, value in default_importance.to_dicts()[0].items() if isinstance(value, float) and value > 0
        ]
        other_features = list(set(vector_features) - set(overwrite_columns))

    with duration("combine-importance-vectors"):
        combined_importance = (
            default_importance.select(pl.col(vector_features) * attribute_vector_weight)
            .vstack(user_importance.select(pl.col(vector_features) * user_vector_weight))
            .sum()
            .select(pl.all() / vector_sum)
        )

        combined_target = pl.concat(
            [default_target.select(overwrite_columns), user_target.select(other_features)], how="horizontal"
        )

    combined_importance, combined_target = await inject_importance_and_target(
        importance=combined_importance, target=combined_target
    )
    combined_target, combined_importance = handle_calorie_concept(combined_target, combined_importance, concept_ids)

    user_importance = combined_importance

    return (
        combined_target,
        combined_importance,
        True,
    )
