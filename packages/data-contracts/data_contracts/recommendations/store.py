from aligned import FeatureStore
from aligned.compiler.model import ModelContractWrapper
from aligned.feature_view.feature_view import FeatureViewWrapper
from data_contracts.preselector.basket_features import HistoricalCustomerMealkitFeatures
from data_contracts.recipe import AllRecipeIngredients, IngredientAllergiesPreferences, RecipeNegativePreferences


def recommendation_feature_contracts() -> FeatureStore:
    """
    Returns all the contracts needed to fulfill the recommendation pipeline.

    This is useful when `FeatureStore.from_dir()` do not work.
    The above will not work when the `personalization` module is loaded in as a package.
    As the working dir is not in the personalization dir, and will not find the contracts.
    """
    from data_contracts.mealkits import DefaultMealboxRecipes, OneSubMealkits
    from data_contracts.menu import MenuWeekRecipeNormalization, YearWeekMenu, YearWeekMenuWithPortions
    from data_contracts.orders import (
        BasketDeviation,
        DeselectedRecipes,
        HistoricalRecipeOrders,
        MealboxChanges,
        MealboxChangesAsRating,
    )
    from data_contracts.preselector.basket_features import (
        ImportanceVector,
        PredefinedVectors,
        PreselectorVector,
        TargetVectors,
    )
    from data_contracts.preselector.menu import PreselectorYearWeekMenu
    from data_contracts.recipe import (
        IngredientCategories,
        MainIngredients,
        NormalizedRecipeFeatures,
        RecipeCost,
        RecipeFeatures,
        RecipeIngredient,
        RecipeMainIngredientCategory,
        RecipeNutrition,
        RecipeTaxonomies,
    )
    from data_contracts.recommendations.recommendations import (
        PartitionedRecommendations,
        PresentedRecommendations,
        RecipeCluster,
        RecommendatedDish,
        UserRecipeLikability,
    )
    from data_contracts.user import UserSubscription

    views: list[FeatureViewWrapper] = [
        RecipeNegativePreferences,
        IngredientAllergiesPreferences,
        AllRecipeIngredients,
        PreselectorYearWeekMenu,
        NormalizedRecipeFeatures,
        RecipeTaxonomies,
        RecipeIngredient,
        HistoricalRecipeOrders,
        BasketDeviation,
        RecipeFeatures,
        RecipeNutrition,
        RecipeCost,
        DeselectedRecipes,
        UserSubscription,
        DefaultMealboxRecipes,
        MealboxChanges,
        MealboxChangesAsRating,
        PreselectorVector,
        YearWeekMenuWithPortions,
        MenuWeekRecipeNormalization,
        PredefinedVectors,
        HistoricalCustomerMealkitFeatures,
        TargetVectors,
        ImportanceVector,
        PartitionedRecommendations,
        OneSubMealkits,
        IngredientCategories,
        RecipeMainIngredientCategory,
        MainIngredients,
        YearWeekMenu,
    ]
    models: list[ModelContractWrapper] = [
        RecommendatedDish,
        RecipeCluster,
        UserRecipeLikability,
        PresentedRecommendations,
    ]

    store = FeatureStore.empty()
    for view in views:
        store.add_compiled_view(view.compile())

    for model in models:
        store.add_compiled_model(model.compile())

    return store
