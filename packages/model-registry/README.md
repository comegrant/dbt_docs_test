# Model Registry

A package to make the model registry experience consistent and less error prone across different environments.

This also enables us to replicate a lot of the capabilities in the Databricks feature store.

## Usage
Run the following command to install this package:

```bash
chef add model-registry
```

### Register a model

Below show-cases how we can register a `sklearn` model.

```python
from mlflow.models.signature import infer_signature
from model_registry import training_run_url

registry: ModelRegistryBuilder = ...

train_X, train_y = ...

model: sklearn.base.BaseModel = ...
model.fit(train_X, train_y)

preds = model.predict(train_y)

await (
    registry
        .signature(
            infer_signature(model_input=train_X, model_output=preds)
        )
        # Can be generated from `training_run_url(...)` which takes a spark session
        .training_run_url("https://url-to-run")
        .training_dataset(train_X)
        .feature_references([
            "mlgold.ml_recipes.cooking_time_from",
            "mlgold.ml_recipes.number_of_taxonomies",
            "mlgold.ml_recipe_nutritional_facts.protein_gram_per_portion",
            "mlgold.ml_recipe_nutritional_facts.carbs_gram_per_portion",
        ])
        .register_as("my_awesome_ml_model", model)
)
```

### MLFlow Registry

To interact with any mlflow registry, either locally or remote. Use the following code to init a registry.

This assumes that you have configured `mlflow` either through env vars, or in code before the registry creation.

```python
from model_registry import mlflow_registry, ModelRegistryBuilder

registry: ModelRegistryBuilder = mlflow_registry()
```

### Databricks Unity Catalog

However, this will not setup `mlflow` to connect to the Databricks Unity Catalog Model Registry. Therefore, change your code to the following.

```python
from model_registry import databricks_model_registry, ModelRegistryBuilder

registry: ModelRegistryBuilder = databricks_model_registry()
```

### Databricks Feature Store

To interact with the Databricks Feature Store registry, use the following code.

```python
from model_registry.databricks_feature_store import databricks_feature_store, ModelRegistryBuilder

registry: ModelRegistryBuilder = databricks_feature_store()
```
