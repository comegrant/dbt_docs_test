# ml-contract-helper

A package to simplify the creating of common ML processes when using a data and model contracts

## Usage
Run the following command to install this package:

```bash
chef add ml-contract-helper
```

### Create a preprocessing pipeline

This package makes it possible to create reasonable default preprocessing pipelines.

```python
from ml_contract_helper import preprocesser_from_contract
from data_contract.some import SomeContract

@model_contract(
    # Selecting all features in the some contract
    input_features=[SomeContract]
)
class SomeModelContract:
    recipe_id = Int32().as_entity()

    label = SomeContract().cooking_time_from.as_classification_label()


pipeline = preprocesser_from_contract(SomeModelContract)

# You can also overwrite the default preprocessing using the following code.
custom_pipeline = preprocesser_from_contract(
    SomeModelContract,
    pipeline_priorities=[
        ("some-feature-tag", Pipeline(...))
    ],
    feature_pipeline=lambda feature_info: Pipeline(...)
)
```

This makes sure that the label is not in the preprocessing pipeline - to avoid data leakage.

Furthermore, it adds common preprocessors based on the data type or if a feature is tagged with different tags.

By default will the used tags be:
- `is_nominal`: adds a most common impute with a one hot encoded transformation
- `is_ordinal`: adds a most common impute with an ordinal encoder - will also add the acceptable_values if present
- `is_interval`: adds a mean impute with a standard scaler
- `is_ratio`: adds a median impute with a standard scaler

There are also some setup based on the datatype
- `float`: adds a mean impute with a standard scalar
- `uint`: adds a median impute with a standard scalar

## Develop

### Installation
To develop on this package, install all the dependencies with `poetry install`.

Then make your changes with your favorite editor of choice.


### Testing
Run the following command to test that the changes do not impact critical functionality:

```bash
chef test
```
This will spin up a Docker image and run your code in a similar environment as our production code.

### Linting
We have added linting which automatically enforces coding style guides.
You may as a result get some failing pipelines from this.

Run the following command to fix and reproduce the linting errors:

```bash
chef lint
```

You can also install `pre-commit` if you want linting to be applied on commit.
