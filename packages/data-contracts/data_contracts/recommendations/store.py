from aligned import FeatureStore
from aligned.compiler.model import ModelContractWrapper
from aligned.feature_view.feature_view import FeatureViewWrapper


def recommendation_feature_contracts() -> FeatureStore:
    """
    Returns all the contracts needed to fulfill the recommendation pipeline.

    This is useful when `FeatureStore.from_dir()` do not work.
    The above will not work when the `personalization` module is loaded in as a package.
    As the working dir is not in the personalization dir, and will not find the contracts.
    """
    from data_contracts.recommendations.recipe import (
        BasketDeviation,
        HistoricalRecipeOrders,
        RecipeIngredient,
        RecipeTaxonomies,
    )
    from data_contracts.recommendations.recommendations import (
        BackupRecommendations,
        PresentedRecommendations,
        RecipeCluster,
        RecommendatedDish,
        UserRecipeLikability,
    )

    store = FeatureStore.experimental()


    views: list[FeatureViewWrapper] = [
        RecipeTaxonomies,
        RecipeIngredient,
        HistoricalRecipeOrders,
        BasketDeviation,
    ]
    models: list[ModelContractWrapper] = [
        RecommendatedDish,
        RecipeCluster,
        UserRecipeLikability,
        BackupRecommendations,
        PresentedRecommendations,
    ]

    for view in views:
        store.add_compiled_view(view.compile())

    for model in models:
        store.add_compiled_model(model.compile())

    return store
