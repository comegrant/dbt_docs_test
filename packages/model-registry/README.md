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

### Inferring new predictions
You can also use a model by using the following code.

```python
registry: ModelRegistryBuilder = databricks_model_registry()

entities = pl.DataFrame({
    "recipe_id": [1, 2, 3],
    ...
})
preds = await registry.infer_over(entities, "some-recipe-model@champion")
```
This will load the needed model and data, and infer new predictions.

However, this assumes that you have set both a model signature and feature references.

If this is not done, is it also possible to pass those into the `infer_over` method.

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
