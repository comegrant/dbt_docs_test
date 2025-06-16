from aligned import ContractStore
from data_contracts.recommendations.store import recommendation_feature_contracts

store: ContractStore | None = None


def load_store() -> ContractStore:
    global store  # noqa
    if store is None:
        store = recommendation_feature_contracts()
    return store
