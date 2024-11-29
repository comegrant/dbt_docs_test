from aligned import ContractStore
from data_contracts.preselector.menu import CostOfFoodPerMenuWeek
from data_contracts.preselector.store import FailedPreselectorOutput, RecipePreferences, SuccessfulPreselectorOutput
from data_contracts.preselector.store import Preselector as PreselectorOutput
from data_contracts.recommendations.store import recommendation_feature_contracts

from preselector.recipe_contracts import Preselector


def preselector_store() -> ContractStore:
    """
    The data-contracts needed to run the pre-selector
    """

    store = recommendation_feature_contracts()

    store.add_feature_view(SuccessfulPreselectorOutput)
    store.add_feature_view(FailedPreselectorOutput)
    store.add_feature_view(RecipePreferences)
    store.add_feature_view(PreselectorOutput)
    store.add_feature_view(CostOfFoodPerMenuWeek)
    store.add_model(Preselector)

    return store
