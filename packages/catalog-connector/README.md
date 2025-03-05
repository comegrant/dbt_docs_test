# Catalog Connector

A package that makes it easy to connect to databricks

## Usage
Run the following command to install this package:

```bash
chef add catalog-connector
```

## Read Data
Below are the different ways to read data.

### Run a SQL query

The easiest way to run a SQL query will be with the following code.
This assumes that you have a Databricks auth token set in an environment variable named `DATABRICKS_TOKEN`. You can also set the workspace by setting the `DATABRICKS_HOST` env var, but this will default to our dev workspace.

> [!TIP]
> If you do not have a `DATABRICKS_TOKEN`, go to the Databricks workspace > Your profile in the upper right corner > Settings > Developer > Access Tokens > Generate new token


```python
from catalog_connector import connection

df = connection.sql("SELECT * FROM dev.mloutputs.preselector_batch WHERE ...")
df.show()
```

### Read a table

You can also use `.table()`, if you rather want to read on specific table.

```python
from catalog_connector import connection

df = connection.table("dev.mloutputs.preselector_batch").read()
df.show()
```

## Write Data
Below are the different ways to write data.

### Append
To append rows to a table use the following.

```python
from catalog_connector import connection

df: spark.DataFrame = ...

connection.table("mloutputs.some_table").append(df)
```

### Overwrite
To overwrite the whole table use the following.

```python
from catalog_connector import connection

df: spark.DataFrame = ...

connection.table("mloutputs.some_table").overwrite(df)
```

### Upsert / Merge
To do an upsert, or sometimes called a merge use the following.

```python
from catalog_connector import connection

df: spark.DataFrame = ...

connection.table("mloutputs.some_table").upsert_on(
    columns=["recipe_id", "portion_id"],
    dataframe=df
)
```

## Session Configuration

If you want to configure your Spark session in another way than the default spark session or a serverless compute, then you have a few options.

```python
from catalog_connector import connection, DatabricksConnectionConfig, EnvironmentValue

# Needs to provide the cluster id, but workspace and token are optional
specific_cluster = DatabricksConnectionConfig.with_cluster_id(
    # You can find this in the Databricks UI and go to. Compute > More (...) > Cluster JSON > cluster_id
    cluster_id="0116-084152-ll1u88aa",
    token=EnvironmentValue("MY_CUSTOM_TOKEN_ENV"),
)

# Can provide the workspace and token if wanted.
serverless_only = DatabricksConnectionConfig.serverless()


# Modify the auth token

my_new_serverless_cluster = serverless_only.with_token("hard-coded-token")
```



# Development

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
