<h1 align="center">
Sous-chef 🧑‍🍳
</h1>

<p align="center">
<img src="assets/sous-chef.png" alt="sous-chef" width="200"/>
</p>

<p align="center">
Sous chef is the Python monorepo for the data team.
</p>


---

## Getting Started
Simply clone the repository and start by installing the `chef` cli tool.

The following script assumes that you have `poetry`.

### Clone Repo
```bash
git clone https://github.com/cheffelo/sous-chef sous-chef
cd sous-chef
```

### Install Python and `chef` cli
```
poetry shell
poetry install
```

This will spin up a new Python virtualenv, and activate the venv in a new shell.
It will also install the core utils (`chef`) to manage the monorepo.

### Create a project
The video bellow shows how to create a new project with `chef` and get started.

For how to setup a Python project either for data science or something else look at [the Python project guide](#python-project)

### Creating New Project and Packages
To create a new project, run the new-service target from the command line and provide a name for your service:

```shell
chef create project
```

To create a new package:
```shell
chef create package
```

For more information about the `chef` cli, view [packages/`chef`/README.md](packages/chef/README.md)

#### What is the difference between \<library name\>, \<package name\> and \<module name\>?

**Library** name is a human readable name. *E.g: Analytics API*

**Package** name is a name without spaces and upper letters for workflows and folders. *E.g: analytics-api*

**Module** name is the python package name which needs underscores. *E.g: analytics_api*

## Use Cases
In this secion will you find a few use-cases to describe how to develop different projects.

### Python Project
In this section will we showcase how to setup a simple ML application that.

#### Create the project
Make sure you have access to the `chef` cli.

> [!NOTE]
> If you do not have access to the `chef` cli. Try running the following:
> ```bash
> poetry shell
> poetry install
> ```

With the chef cli activated, run the following to create a new project:

```bash
chef create project
```

This will prompt you for different questions. However, it is mainly the `Project Name` that needs to be inputted. For the rest can you press `Enter`, unless you want to customise it futher.

The command will add a basic project structure needed for Python development, and a few files for basic development locally, and on Databricks.

#### Start developing
We are now ready to start adding our custom code.

Open up a new shell / terminal, and move into the project directory. This can be done by running:

```bash
cd projects/<your-project-name>
```

This project will have it's one Python environment, which prohibits conflicting Python packages across projects, but it also enable us to use different Python versions per project.

As a result, we need to create a new virtual environment, and install the project packages again. Therefore, run the following:

```bash
poetry shell
poetry install
```

We can now add external and internal packages with:

```bash
chef add data-contracts  # Internal package at `packages/data-contracts`
chef add streamlit       # External UI package
```

#### Example file

To showcase a simple example. Add the following streamlit app to `app.py`. This will search for recipe embeddings in a vector database.

```python
import asyncio
import streamlit as st

from project_owners.owner import Owner

from data_contracts.recommendations.recipe import RecipeFeatures
from data_contracts.recommendations.store import recommendation_feature_contracts

from aligned import feature_view, String, Bool, FileSource, model_contract
from aligned.exposed_model.ollama import ollama_embedding_contract
from aligned.sources.lancedb import LanceDBConfig

vector_db = LanceDBConfig(path="./vector_db")

recipe = RecipeFeatures()

RecipeEmbedding = ollama_embedding_contract(
    input=recipe.recipe_name,
    entities=recipe.recipe_id,
    model="nomic-embed-text",
    endpoint="http://our-embedding-service:11434",
    contract_name="recipe_embedding",
    contacts=[Owner.matsmoll().markdown()],
    output_source=vector_db.table("recipe_embeddings").as_vector_index("recipes")
)

async def main():
    recipe_to_search = "Laks med soya og ris"
    st.write(f"Searching for '{recipe_to_search}'")

    store = recommendation_feature_contracts()
    store.add_model(RecipeEmbedding)

    with st.spinner("Creating Embeddings"):
        await store.model("recipe_embedding").predict_over(
            RecipeFeatures.query().all()
        ).insert_into_output_source()

    similar_recipes = await (
        store.vector_index("recipes")
            .nearest_n_to({
                "recipe_name": [recipe_to_search],
                number_of_records=5
            }).to_pandas()
    )

    st.title("Similar recipes")
    st.write(similar_recipes)

if __name__ == "__main__":
    asyncio.run(main())
```

#### Run the project
With a small application can we run the project locally through docker. Howerver, we need to add the startup command to the `docker-compose.yaml` file first.

```yaml
services:
  app:
    platform: linux/amd64
    build:
      context: ../../
      dockerfile: projects/<project-name>/docker/Dockerfile
    volumes:
      - ./:/opt/projects/<project-name>/
      - ./../../packages:/opt/packages
    command: "python -m streamlit run app.py --server.fileWatcherType poll"

    depends_on:
      - base
    ports:
      - 8500:8501
    env_file:
      - ../../.env
      - .env

    ...
```

Now startup the application with:
```bash
chef up app
```
This will build the project, install everything that is needed and start up the server at `http://127.0.0.1:8500`.

### Data Science

Data science applications are a subtype of a Python project. Meaning you can use everything described in the Python Project use-case.
However, to manage the unpredicability of data and ML could the following also be needed:

 - Experiment tracking
 - Model versioning - through a model registry
 - Feature store - to load offline point-in-time data, and low latency online data.
 - Big Brain compute - aka. extra RAM / disk
 - Out of memory compute - through Spark / distributed processing
 - Job orchestration
 - Model serving enpoint
 - Monitor and validate data - either data drift or semantic expectations
 - Evaluate model online performance
 - Explain model outputs

For all of this do we default to the Databricks' components.

Meaning `MLFlow`, `Spark`, Databricks' `feature-engineering` package, `Databricks Asset Bundles`.
However, we still use `Docker` to controll the dependencies through the `docker/Dockerfile.databricks` file. See the [databricks-env README](https://github.com/cheffelo/sous-chef/tree/main/packages/databricks-env) for a more details.
