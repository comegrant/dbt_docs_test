from aligned import FeatureStore
from aligned.retrival_job import SupervisedJob
from aligned.schemas.folder import DatasetMetadata
from data_contracts.recommendations.recipe import HistoricalRecipeOrders
from datetime import datetime

from {{cookiecutter.module_name}}.model_contract import YourModelContract, model_data_dir
from {{cookiecutter.module_name}}.data_models import {{cookiecutter.module_name}}_contracts



async def load_dataset(store: FeatureStore, run_timestamp: datetime):

    formatted_run_timestamp = run_timestamp.isoformat()
    data_metadata = DatasetMetadata(
        id=formatted_run_timestamp,
        name="{{cookiecutter.library_name}}",
        description="{{cookiecutter.project_description}}",
        tags=["example-tag"]
    )

    # entities = {
    #     "recipe_id": [1, 2, 3, ...]
    # }
    entities = HistoricalRecipeOrders.query().all(limit=1000)

    model_feature_store = store.model(YourModelContract.metadata.name)

    freshness = await model_feature_store.freshness()
    print(f"Loading dataset for model {model_feature_store.model.name} using data updated at {freshness}")


    data = (
        model_feature_store
        .with_labels()
        .features_for(
            entities,
            event_timestamp_column=None, # Set this for point in time valid input features
            target_event_timestamp_column=None, # Set this for point in time valid target features
        )
        .cache_raw_data(
            model_data_dir.directory("raw_data").parquet_at(f"{formatted_run_timestamp}.parquet"),
        )
    )

    datasets = data.train_test_validate(
        train_size=0.8,
        validate_size=0.1,
    )

    if model_feature_store.model.dataset_store:
        dataset_dir = (model_data_dir
            .directory("datasets")
            .directory(formatted_run_timestamp)
        )

        datasets = datasets.store_dataset(
            model_feature_store.model.dataset_store,
            metadata=data_metadata,
            train_source=dataset_dir.parquet_at("train.parquet"),
            test_source=dataset_dir.parquet_at("test.parquet"),
            validate_source=dataset_dir.parquet_at("validation.parquet"),
        )

    return datasets



async def train_model(dataset: SupervisedJob):
    from sklearn.ensemble import RandomForestClassifier

    loaded_dataset = await dataset.to_polars()

    model = RandomForestClassifier()
    model.fit(
        loaded_dataset.input.collect().to_pandas(),
        loaded_dataset.labels.collect().to_pandas()
    )

    return model


async def evaluate_model(dataset: SupervisedJob, model) -> None:
    # Load the dataset
    loaded_dataset = await dataset.to_polars()

    preds = model.predict(loaded_dataset.input.collect().to_pandas())
    target = loaded_dataset.labels.collect().to_pandas()

    # Your evaluation code here


async def run():
    run_timestamp = datetime.now()
    store = {{cookiecutter.module_name}}_contracts()

    dataset = await load_dataset(store, run_timestamp)

    model = await train_model(dataset.train)

    evals = [
        ("Test", dataset.test),
        ("Validation", dataset.validate)
    ]
    for name, dataset in evals:
        print(f"Evaluating model on {name} dataset")
        await evaluate_model(dataset, model)
