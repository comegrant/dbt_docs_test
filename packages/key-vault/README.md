# Key Vault

A package to simplify how to load key vault values into a python application.
This assumes that you are using `pydantic.BaseSettings` to define all your secret requirments.

## Usage
Run the following command to install this package:

```bash
chef add key-vault
```
To showcase how we can use the secrets. let's assume that we want to setup DataDog logging.
Therefore, we have the following settings that we need:

```python
class DataDogConfig(BaseSettings):
    """
    The values needed to setup datadog as a logger.
    """
    datadog_api_key: str
    datadog_service_name: str
    datadog_tags: str

    datadog_site: Annotated[str, Field] = "datadoghq.eu"
    datadog_source: Annotated[str, Field] = "python"
```
With this we can load the values from our key vault.

However, it can differ based on where you run the code.

### Databricks
If you are running a workflow in databricks can you use the `DatabricksKeyVault`.

```python
vault = DatabricksKeyVault.from_scope("auth_common")

settings = await vault.load(DataDogConfig)
```

### Azure
If you are running your code as a streamlit app, locally, or anywhere outside of Databricks, then the `AzureKeyVault` may be a better fit.

```python
vault = AzureKeyVault.from_vault_name("kv-chefdp-common")

settings = await vault.load(DataDogConfig)
```

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
