from aligned import FeatureStore

from customer_churn.model_contract import MyFeatures, YourModelContract


def customer_churn_contracts() -> FeatureStore:
    store = FeatureStore.experimental()

    views: list = [
        MyFeatures,
    ]
    models: list = [YourModelContract]

    for view in views:
        store.add_compiled_view(view.compile())

    for model in models:
        store.add_compiled_model(model.compile())

    return store
