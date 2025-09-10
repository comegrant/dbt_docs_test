# Data Transformation Guide
This document provides guidance when creating data models. To transform the data we use dbt (data build tool).

## Table of Contents

1. [📂 Folder Organization](#-1-folder-organization)
2. [🛠️ Intro to dbt](#️-2-intro-to-dbt)
3. [📚 Documentation](#-3-documentation)
4. [📝 Code Guidelines](#-4-code-guidelines)
5. [🏷️ Naming Conventions](#️-5-naming-conventions)
6. [⚙️ Metadata and Configurations](#️-6-metadata-and-configurations)
7. [🔄 Transformations](#-7-transformations)
8. [💻 Development](#-8-development)

## 📂 1. Folder Organization

Below is the file tree of the dbt project. It follows the general structure of dbt projects, which is also explained in [their own documentation](https://docs.getdbt.com/docs/build/projects).

```
transform
├──analyses/                                                      # Can be used to store ad hoc queries (not currently in use)
├──dbt_packages/                                                  # Files end up here after running dbt deps in the terminal
├──logs/                                                          # Logs created by dbt
├──macros/                                                        # Helper macros for common transformation steps
├──models/
│   ├── silver/                                                   # Silver layer
│   │   └── source_system/                                        # Folder for each source system
│   │   │   ├── _<source_system>_docs.md                          # Column documentation
│   │   │   ├── _<source_system>_models.yml                       # Reference to models in silver
│   │   │   ├── _<source_system>_source.yml                       # Reference to source tables in bronze
│   │   │   └── <source_system>__≤model_name>.sql                 # Model SQL
│   ├── intermediate/                                             # Intermediate logic
│   │   └── business_concept/                                     # Folder for each business concept
│   │   │   └── domain/                                           # Folder for more specific parts of the domain
│   │   │   │   └── int_<name_describing_model_output>.sql        # Model SQL
│   ├── gold/                                                     # Gold layer
│   │   └── business_concept/                                     # Folder for each business concept
│   │   │   ├── _<source_system>_docs.md                          # Column documentation for columns added after silver
│   │   │   ├── _<source_system>_models.yml                       # Reference to models in gold
│   │   │   └── dim_<dim_name>.sql                                # Model SQL for dimension
│   │   │   └── fact_<fact_name>.sql                              # Model SQL for fact
│   ├── diamond/                                                  # Models exposed to external systems
│   ├── mlgold/                                                   # ML-specific features
│   └── drafts/                                                   # Temporarily stored old model code
├──profiles/                                                      # Profile configurations used in the CI/CD
├──seeds/                                                         # CSV files with static data that you can load into your data platform with dbt
├──snapshots/                                                     # A way to capture the state of your mutable tables so you can refer to it later
├──tests/                                                         # Test queries for singlar and generic tests
├──states/                                                        # Manifests of dev, test and prod used for the --defer --state flags
├──target/                                                        # Compiled code
└──dbt_project.yml                                                # Project wide configs
```

## 🛠️ 2. Intro to dbt
This [video](https://www.youtube.com/watch?v=5rNquRnNb4E&list=PLy4OcwImJzBLJzLYxpxaPUmCWp8j1esvT) provides a good introduction to dbt. Note that much of the setup covered at the beginning of the video is not needed as we already have a git repository with a dbt project.

In addition [this best practice guide](https://docs.getdbt.com/best-practices) can be useful to look into to learn more about dbt. Mark that we do not follow all the same guidelines and do not use their semantic layer for metrics nor dbt mesh.

> [!NOTE]
> dbt has a different setup than us in their examples and best practices. Below is a translation to avoid confusion when reading docs:
>
> Staging = Silver
>
> Intermediate = Intermediate
>
> Marts = Gold

## 📚 3. Documentation
All tables and columns are documented in Databricks and visible in the catalog of the workspace. To see data lineage as well as column and table documentation, it's also possible to generate documentation for the dbt project using the commands below. [Read more about documentation in dbt here](https://docs.getdbt.com/reference/commands/cmd-docs).
- `dbt docs generate`
- `dbt docs serve`

Ensure that your virtual environment is activated, and that you are inside the transform folder before generating the docs:
- `poetry shell` or `source .venv/bin/activate` (if using poetry>=2.0.0) 
- `cd transform`

## 📝 4. Code Guidelines

### 4.1 General structure and conventions
- ✅ Use lower case for all syntax
- ✅ Use leading commas
- ✅ Use left joins together with where clauses, rather than inner joins 
- ✅ Group implicitly by using `group by 1, 2, 3 etc` or `group by all`. [Read more](https://www.getdbt.com/blog/write-better-sql-a-defense-of-group-by-1).
- ✅ Split code into CTEs. [Read more.](https://docs.getdbt.com/terms/cte)

#### CTEs
The model code should be structured using CTEs to make the code more modular. The goal is to create [DRY code](https://www.getdbt.com/blog/dry-principles) and make it easy for the person coming after you to understand what's happening.

Please follow the following structure:
- All models code should start with referencing the source models using a `select *`

```SQL
with 

short_name as (

    select * from {{ref('model_name')}}

)
```

- The source model CTEs should be sorted after the layer of the source (silver -> intermediate -> gold).
- After the source CTEs there should be one CTE for each bigger transformation step.
- Name the CTEs so that it's easy for an external person to understand the purpose of the CTE. Read more under [Naming Conventions](#Naming-Conventions)
- Use comments *above* the CTE if extra explanation is needed.
- CTEs should be organized in a way that makes it easy to follow the steps performed in the code.

## 🏷️ 5. Naming Conventions

### 5.1 Models

**Silver:** `<source_system>__<source_table_name>(s).sql`: 

Keep table name from bronze but change the name to be in plural if not already, unless it ends with "ledger" or something similar that does not make sense in plural.

**Intermediate:** `int_<output_granularity>_<extra_info>s.sql`: 

The name should make it easy for other developers to understand the goal of the intermediate table. 

Examples:
- `int_agreements_first_order`: Finds the first order of each agreement
- `int_order_lines_orders_joined`: Joins billig_agreement_order_lines and billing_agreement_orders
- `int_menu_weeks`: Contains all menu weeks on menu week level
- `int_agreements_customer_journey_sub_segments`: Add the customers journey sub segments to each agreement


**Gold:** `dim/fact_<business_concept>.sql` 

The model file should start with fact or dim based on the type of table followed by a logical business-related name in plain English. Its does *not* have to represent the granularity of the table.

Examples:
- `fact_orders`
- `fact_preselector_output`
- `dim_products`
- `dim_recipes`

### 5.2 CTEs
The name of the CTEs should make it easy for the reader to understand the goal of the output of the CTE. 

Hence, we suggest the following structure: 
- `<output_granularity>_<extra_info>`. 

Avoid making the name to long, but also avoid making the name to generic.

### 5.3 Columns
When naming columns the following naming conventions should be used:
- 🐍 Use snake_case
- 🤷🏻‍♀️ Avoid generic name, use `product_name` rather than `name` or `product`.
- 🚫 Avoid using [reserved words](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-reserved-words).
- 📅 Dates: `<event>_date` using past tents for event. Example: `created_date`.
- ⏱️ Timestamps: `<event>_at` using past tents for event. Example: `created_at`.
- 🎚️ Booleans: Prefixed with `is_`, `has_`. Example: `is_active_customer`, `has_admin_access`.
- 🔑 ID columns: `<table_name>_id`. Example: `billing_agreement_id`.
- 🚧 Source system fields: Add `source_` as prefix. Example: `source_created_at`.
- 💪 Be consistent in namings:
  - billing_agreement *not* customer/subscriber etc
  - menu_year *not* order_week/delivery_week
  - order = order made by billing agreement
  - purchase = procurement related order

## ⚙️ 6. Metadata and Configurations
This part of the documentation explains how we work with configuarions and metadata in dbt.

### 6.1 dbt-project.yml
The dbt-project.yml file holds project wide configurations and variables that are used across the code.

**Variables**: Variables can be used to store information such as IDs and dates that are commonly used in the code. It's not needed to create variables unless the value will be referred to several times across different models.

📖 [Read More about variables in dbt here](https://docs.getdbt.com/docs/build/project-variables)

### 6.2 sources.yml
All silver subfolders have a source.yml file. When creating a silver model, you need to add the bronze table you are working with to this file. 

📖 [Read More about sources in dbt here ](https://docs.getdbt.com/docs/build/sources)

### 6.3 models.yml
All silver and gold subfolders have model.yml files. The model table is used to set up model-specific configurations and metadata as well as data tests. All silver and gold models and the related columns should be added to the models.yml file. Below is the required setup for silver and gold models. 

📖 [Read more about models properties in dbt here](https://docs.getdbt.com/reference/model-properties).

#### Silver
Silver models should have a good description, contracts set to true, and relevant [data tests](https://docs.getdbt.com/docs/build/data-tests) on the columns.

```yml
  - name: <source_system>__<table_name>
    description: "Well written description of table"
    config:
      contract:
        enforced: true

    columns:
      - name: column_name
        data_type: <int/string/decimal(38,2)>
        description: "{{ doc('column__<column_name>') }}"
        data_tests:
          - unique
          - not_null

```

#### Gold
Gold models should have a good description, version, contracts set to true, constraints for primary keys and foreign keys, and relevant [data tests](https://docs.getdbt.com/docs/build/data-tests). In addition, each gold model must have at least one specified owner. The owner is responsible for maintaining the model or delegating to another team member.

```yml
  - name: fact/dim_<name>
    description: "Good description"
    meta:
      owners:
      - "name"  # Owner 1
      - "name"  # Owner 2 (optional)
    latest_version: 1
    config:
      alias: fact/dim_<name
      contract:
        enforced: true

    columns:
      - name: pk_fact_table_name
        data_type: string
        description: "{{ doc('column__pk_fact_table_name') }}"
        constraints:
          - type: primary_key
          - type: not_null
        data_tests:
          - unique
          - not_null

      - name: column_name
        data_type: int
        description: "{{ doc('column__column_name') }}"
        data_tests:
          - not_null

      - name: fk_dim_example
        data_type: string
        description: "{{ doc('column__pk_dim_example') }}"
        constraints:
          - type: not_null
          - type: foreign_key
            to: ref('dim_example')
            to_columns: [pk_dim_example]
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_example')
              field: pk_dim_example

      - name: fk_dim_another_example
        data_type: string
        description: "{{ doc('column__pk_dim_another_example') }}"
        constraints:
          - type: foreign_key
            to: ref('dim_another_example')
            to_columns: [pk_dim_another_example]
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_another_example')
              field: pk_dim_another_example

```

### 6.4 docs.md
Each subdirectory of the layers has a `docs.md` file. This is used to store column documentation in [doc blocks](https://docs.getdbt.com/docs/build/documentation#using-docs-blocks). The purpose of this is to be able to reuse column documentation across models.

For columns that exist across tables, they should be placed under the heading of the main source. For example, the description of `product_variation_id` should be placed under the heading "Product Variations" and not the heading "Menu Variations". If there is no natural main source, `__common_docs.md` can be used and a suitable heading should be created.

Columns that gets create in intermediate or the gold layer should be documented in the docs.md-files of the gold models. Likewise with columns created in the mlgold or diamond layer.

**Example:**
```
# Products
{% docs column__product_id %}

The primary key of the products in the product layer database.

{% enddocs %}

{% docs column__product_name %}

The name of the products in the product layer database.

{% enddocs %}

{% docs column__product_description %}

The description of the products in the product layer database.

{% enddocs %}

```

### 6.5 Model Materialization
The [materialization](https://docs.getdbt.com/docs/build/materializations) of the models is initially determined in the dbt_project.yml file. However, deviations from the default can be made by configuring the materialization within the SQL file of the model. We currently use table-materalization most of the time, and incremental in some silver tables.

> [!WARNING]
>If the ingestion of a table is made using incremental load, the silver table also need incremental load.

### 6.6 Snapshots
Sometimes [snapshot](https://docs.getdbt.com/docs/build/snapshots) need to be made to source tables to track history. When that is the case the snapshot table must be added between the bronze and silver layer. This is done in the `models/snapshots` folder.

Snapshots are needed if the source system updates and overwrites data instead of appending, and the changes made are of interest to track. Sometimes the source system updates and overwrites the data, but it's still not of any interest to track the changes. Please consider the overall business question we want to answer before creating a snapshot.

## 🔄 7. Transformations
This section explains which transformations that are appropriate in each layer.

### 7.1 Silver
**Purpose:** Clean and standardize raw data

**Transformations:**
- ✅ Renaming, remember the [🏷️ Naming Conventions](#53-columns).
- ✅ Type casting.
- ✅ Basic computations (e.g., cents to dollars, add VAT, etc.).
- ✅ Consistent casing of strings. Use initcap(), upper() or lower() to ensure consistent casing of strings where it makes sense.
- ✅ Remove columns that are not relevant. We do not want to include columns from the source that will never be in use.

**Avoid:**
- ❌ Joins
- ❌ Unions
- ❌ Aggregations

**Exceptions:**
- If the silver layer has historic data which comes from another source table, these should be combined in the silver layer, and unions or joins can be needed. In this case, the legacy source table should be handled in the `base` folder before it's combined with the silver model. [Read more about base tables here](https://docs.getdbt.com/best-practices/how-we-structure/2-staging#staging-other-considerations) (staging = silver).

> [!WARNING]
>Silver models should never reference other silver models.
>
>Their source should always be a bronze table, snapshot table, or a base table.

### 7.2 Intermediate
**Purpose:** Modularize code to be reused across the gold layer or other intermediate tables. 

**Transformations:**
- ✅ Join silver tables
- ✅ Perform advanced calculations
- ✅ Pivot tables
- ✅ Filter data

### 7.2 Gold
**Purpose:** Create analytics-ready dimensional model

**Transformations:**
- ✅ Join silver and/or intermediate tables
- ✅ Add primary keys
- ✅ Add foreign keys
  - Add foreign keys by concatenating columns and performing the needed transformations to create the key
  - Only when needed, join in the dim table to extract foreign key
- ✅ Other needed transformations that do not belong to the intermediate layer

Avoid:
- ❌ Join with other facts (then the needed logic should be moved to the intermediate layer)

>[!NOTE]
>Keep the ID columns and other columns used to create foreign keys in the table as they are useful for QA or if doing ad hoc analysis where gold and silver tables are combined.

### 7.3 Diamond
**Purpose:** Refinement of data that is sent outside of the data plattform

**Transformations**
- ✅ Join silver/intermediate/gold tables
- ✅ Aggregations

## 💻 8. Development

### 8.1 Feature Branch and Virtual Environment
We use [feature branches](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow) when developing. In addition, we align the development setup across developers leveraging virtual environments using [Poetry](https://python-poetry.org/docs/).

Please ensure to go through the following steps when starting to work on a new feature:

1. Created a new branch based on main
- Go to main: `git checkout main`: go to main
- Get the newest changes from main: `git pull`
- Create new branch: `git checkout -b firstname/branch-name`

2. Activate the virtual environment from the data model folder. The virtual environment from last time you developed might still be active, if not perform the step below:
- Poetry<2.0.0: `poetry shell`
- Poetry>=2.0.0: `poetry source .venv/bin/activate` 

3. Enter the transform folder: `cd transform`

### 8.2 Developer Schema
All developers have their own schema in the Databricks Dev Workspace. When dbt models are built locally on your machine they get populated to the developer schema. The name of the schema is set when configuring the [dbt profile](../README.md#-3-getting-started).

To ensure a smooth and consistent experience when developing with dbt locally, we provide a helper command: 
```
chef dbt prepare-local-development
```

The command performs two key actions:
1.  Drops tables in your personal schemas
It scans available schemas and removes all tables and views in the schemas that match your personal schema prefix. This is especially useful for cleaning up stale or temporary development artifacts. NB! This will **permanently drop** all tables and views in your personal schemas.

2. Prepares states for deferred builds
After cleaning up, the script regenerates `manifest.json` files for the three targets `dev`, `test` and `prod`, and places them in the folders `states/dev`, `states/test` and `states/prod`, respectively. 
These states are used for deferred resolution in dbt, which is explained [further down](#84-build-and-run-models).

It's recommended to use the command whenever starting on a new feature branch.

### 8.3 Code Generation

When developing, you can auto-generate code leveraging our custom CLI commands.

#### Create code for silver tables
When creating silver tables, we have a command which allows you to get a head start on adding the model. The models.sql-file will be created when running this script. There are no code generation command for models in other layers as there is no standard setup for these models.

```
chef dbt generate-silver-model \
--source-system <source_system> \ # name of the source system, e.g., cms
--source-table-name <source_table_name> \ # name of the bronze table, e.g., cms__company
--model-name <model_name> # name of the silver model, e.g., cms__companies
```

After this, you need to:
- Organize the columns by data type
- Rename columns
- Remove columns that are not relevant
- Perform other cleaning and/or transformation steps

#### Create docs for silver models
There is a command which helps you generate docs blocks. Before this can be used, you need to add the model (only model, and not columns) to the model.yml-file. Note that the models must have been [built successfully](#build-and-run-models) for this command to work. This is mainly useful for silver models as models in other layers often have most of their columns already documented in the silver layer documentation. New column that are added in other layers should be added to the column documentation of the gold/mlgold/diamond layer.

```
chef dbt generate-docs --model-name <model_name> # name of the silver model
```

This will provide you with an output in the terminal of all the columns in the silver model represented as doc blocks. After this the following must will be done:
- Create a heading and paste this into the relevant docs.md file.
- Delete the docs that are already present under other headings.
- Move columns to other headings of the table they originate from.

>[!NOTE]
>The documentation will be visible in the catalog in Databricks. You can also view the documentation by running `dbt docs generate` followed by `dbt docs serve` in the terminal. Note that for this to be possible, the next step must be done first.

#### Create column schema for all models
After the docs have been generated, this command below be used to generate the column schema for the models.yml file. This command can be used for models across all layers. Note that the models must have been [built successfully](#build-and-run-models) for this command to work.

```
chef dbt generate-yaml --model-name <model_name> # name of the model
```

1. Copy the output and add it to the relevant `__models.yml` file.
2. Double check data types
3. Remove the generated data tests that are not relevant for the columns
4. Add more [data tests](https://docs.getdbt.com/docs/build/data-tests) if needed

### 8.4 Build and run models
When you have created or made changes to a model, you need to test that it works by running it. After the model is run, you can find the table in your development schema in the Databricks Catalog.

To run a model, you can use the following commands: 
- `dbt run -s model_name`: runs the model you refer to
- `dbt run -s +model_name`: runs the model you refer to and all the upstream models
- `dbt run -s model_name+`: runs the model you refer to and all the downstream models

If replacing `run` with `build`, the code will both be run and tested. You need to run with `build` successfully before the code can be considered done. However, it can be useful to only use `run` when making only small changes and testing the output. [Read more about dbt CLI commands here](https://docs.getdbt.com/reference/dbt-commands)

#### Clone
When working with gold models, there often exist relationships between tables. In this case, it can be useful to [clone](https://docs.getdbt.com/reference/commands/clone) the gold tables from test or prod into your developer schema using the following commands:

Clone gold tables from prod to your personal schema, while excluding the table you are modifying
```
dbt clone -s models/gold --exclude model_name --state states/prod
```

Clone gold tables from prod to your personal schema when creating a new table
```
dbt clone -s models/gold --state states/prod
```

#### Defer State
When working with models that depend on other models, it's very useful to use [defer](https://docs.getdbt.com/reference/node-selection/defer). This will tell dbt to fetch data from test or prod to create the models you are building, if the source table does not exist in your own developer schema.

Build a specific model and defers to all the models that it depends on (which are not already in the developer schema):
```
dbt build -s model_name --defer --state states/test
```

Runs a specific model and defers to all the models that it depends on (which are not already in the developer schema):
```
dbt build -s model_name+ --defer --state states/test
```

>[!NOTE]
>To be able to defer, you must first have prepared your developer schema using `chef dbt prepare-local-development`. 
>
>This is because defer needs to use the manifests that are stored in the states folder.

> [!TIP]
> Replace `test` with `prod` if you want to defer from prod instead.

### 8.5 Tests
dbt has a good setup for testing that should be used when relevant. Please read more about tests in the [dbt documentation](https://docs.getdbt.com/docs/build/data-tests).

### 8.6 Debugging
After deploying a model the output can be inspected by querying Databricks.

If you wonder how the query of your dbt model looks like you can also run `dbt compile -s model_name` in your terminal, you will get the compiled code.

After each run of a model the compiled code is also stored in the target-folder of the dbt-project.

The compiled code can be useful when debugging, either by reading the code and checking that it compiled as expected or copy-pasting it to Databricks, running the code there and making adjustments while debugging.

### 8.7 Creating a pull request
When you are done with your code and have tested that it works locally, you can create a pull request and assign a reviewer. 

When a pull request is created, the models you have modified and all downstream models will be built in a pull request schema prefixed with `~PR####_` where `####` is the number of your PR. After the PR is merged, the pull request schemas will be dropped. The code will be moved to the test workspace, but not built automatically. After this, it will be moved to the prod workspace when some of the admins approve the deployment.

> [!TIP]
>If you want to see how your code looks in GitHub, but are not ready to create a pull request yet, you can create a [draft pull request](https://github.blog/news-insights/product-news/introducing-draft-pull-requests/)

### 8.8 Development process step by step
Below is a step-by-step guide for development that can be used as a reminder while working. You can also use this [Miro board](https://miro.com/app/board/uXjVLIMsa8g=/?share_link_id=722109664611) for reference. Mark that one should often separate these steps into several feature branches and PRs.

1. Ingest table to bronze following the guidelines in the [Ingestion Guide](01_INGESTION.md)
2. Prepare local development: `chef dbt prepare-local-development`
3. Add table(s) of interest to the `_source_system__source.yml`-file
4. Create snapshot table (if needed)
5. Create base table (if needed)
6. Create silver model
  - Reorganize and remove columns that are not needed
  - Rename columns following the [naming conventions](#naming-conventions)
  - Perform basic transformation steps if needed
7. Run silver model: `dbt run -s model_name`
8. Check that output is as expected (query the table in Databricks)
9. Add the silver model(s) to the `_source_system__models.yml`-file
10. Add column documentation to the `_source_system__docs.md`-file
11. Add columns to the `_source_system__models.yml`-file
  - Remove unnecessary tests from columns
  - Add more [tests](https://docs.getdbt.com/docs/build/data-tests) if needed
12. Set up incremental load (if needed)
13. Run and test silver model: `dbt build -s model_name`
14. Create intermediate tables (if needed)
15. Run and test intermediate model: `dbt build -s model_name --defer --state states/prod`
16. Check that output is as expected (query the table in Databricks)
17. Clone gold models if needed: `dbt clone -s models/gold --state states/prod`
18. Create gold tables
19. Run gold model: `dbt run -s model_name --defer --state states/prod`
20. Check that output is as expected (query the table in Databricks)
21. Add the gold model(s) to the `_business_entity__models.yml`-file
22. Add column documentation to the `_business_entity__docs.md`-file
23. Add columns to the `_business_entity__models.yml`-file
24. Run and test gold model: `dbt build -s model_name --defer --state states/prod`
25. Check that output is as expected (query the table in Databricks)
