from aligned import FeatureStore

from {{cookiecutter.module_name}}.model_contract import YourModelContract, MyFeatures

def {{cookiecutter.module_name}}_contracts() -> FeatureStore:
    store = FeatureStore.experimental()

    views: list = [
        MyFeatures,
    ]
    models: list = [
        YourModelContract
    ]

    for view in views:
        store.add_compiled_view(view.compile())

    for model in models:
        store.add_compiled_model(model.compile())

    return store

