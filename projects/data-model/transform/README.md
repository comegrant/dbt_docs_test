# Table of Contents

1. [Data Model Guidelines](#1-data-model-guidelines)
2. [Development Process](#2-development-process)
3. [Working with developer schemas](#3-working-with-developer-schemas)
4. [Setting up dbt for the first time](#4-setting-up-dbt-for-the-first-time)

# 1. Data Model Guidelines
This section describes how the data model is organized and the guidelines for data modelling.

## 1.1 Terminology
- `models`: The sql-script which does the transformation to the data
- `tables`: The result of a model that is populated to Databricks
- `layers`: The different levels of transformed models
- `dbt project`: The folder called transform and belonging files

## 1.2 Layers
In our data model we have four layers which is represented by a folder under `models/` in the dbt project.

| Layers | ⭐️ Purpose | 📁 Subdirectories | 📃 Model names |
| ----------- | ----------- | ----------- | ----------- |
| 🥉 Bronze | Contains raw data | None, only exists in the Databricks Workspace | Same as in the source but with a prefix of the source `<source_system>__` |
| 🥈 Silver | Cleanse and standarize raw tables from bronze | Folders divided by source system | `<source_system>__<source_table_name>(s).sql`: Keep table name from the source, but all words except the last should be singlar and the last should always be plural unless it end with "ledger" or something similar that does not make sense in plural |
| 🏄🏻 Intermediate | Modularize transformations that is reused across other intermediate and gold models | Folders divided by business groupings | `int_<silver_model_reference>_<actions>s.sql`: The name should make it easy for other developers to understand the main acitons done in the intermediate step and which silver models it involves. Look at existing intermediate tables for inspiration. |
| 🥇 Gold | Join and transform tables from silver and intermediate to create a dimension or fact table | Folders divided by business groupings | `dim/fact_<business_concept>.sql`: The model file should start with fact or dim based on the type of table followed by a logic business related name in plain english. One should NOT create several models for the same concept. I.e., there should not be a table for `finance_orders` and `marketing_orders`. |
| 🤖 mlGold | Perform further transformations and aggregations needed for specific ml projects | Not decided yet | Not decided yet |

## 1.3 Naming Conventions
This is the naming conventions that should be followed when creating models:
- 🐍 Use snake_case
- 🚫 Avoid using reserved words (such as [these](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-reserved-words) for Azure Databricks)
- 📅 Date should be named as `<event>_date`, e.g. `created_date`
- ⏱️ Timestamp should be names as `<event>_at`, e.g. `created_at`
- 🔙 Events dates and times should be past tense — created, updated, or deleted.
- 🎚️ Booleans should be prefixed with `is_` or `has_` etc., e.g., `is_active_customer` and `has_admin_access`
- 🔑 Id columns that are used as primary keys in the source system should always be called `<table_name>_id` e.g `billing_agreement_id`
- 🤷🏻‍♀️ Avoid too generic names, e.g., use `product_name` rather than `name` or `product`.
- 💪 Consistency is key! Use the same field names across models where possible. For example, an id to the `billing_agreement` table should be named `billing_agreement_id` everwhere rather than alternating with `customer_id`
- 🚧 System fields from the source system should have source_ as prefix, e.g., the created_at and created_by columns should be source_created_at and source_created_by.

## 1.4 Best Practices
Please follow these best practices when creating models:
- Use lower case for all code
- Use leading commas
- Do not use abbreviation in aliases and CTEs, i.e, use `order_lines` rather than `baol`
- Only use inner joins when you have a very good reason, left joins combined with where clauses for filtering is prefferd as it's easier to keep track of what's being done.
- Grouping (and ordering) should be done implisitt by either using `group by 1, 2 ,3 etc` or `group by all`. Read more about this [here](https://www.getdbt.com/blog/write-better-sql-a-defense-of-group-by-1).

# 2 Development Process
This section describes how to develop models using dbt.

## 2.1 Create a new branch
Before starting to develop it's very important that you have created a new branch. Follow the description below:
* Go to main: `git checkout main`
* Pull from main: `git pull`
* Create new branch: `git checkout -b "your-firstname/whatever-you-want-to-call-your-branch"`
* Navigate to to transform: `cd projects/data-model/transform` (this depends on which folder you are currently in)

## 2.2 Add source table to the dbt project
If you are adding tables to the silver layer you need to start by creating a reference to the source table in the bronze layer. This ensure that one can refer to the source table in when creating a model by using the [source()-function](https://docs.getdbt.com/reference/dbt-jinja-functions/source).

Each source system has a subdirectory in `models/silver` where there is a .yml-file with the suffix `__sources.yml`. To add the reference to a source table you need to add the following line into the bottom of this file of the source system you are getting the table from.

`- name: insert table name in bronze`

>[!NOTE]
>Make sure that the tables of interest have been inserted to bronze. If not you need to follow the steps of the README in the [ingest-folder](LINK HERE)

## 2.3 Create model

### a. Create model file
For the silver layer the model file and some default code can easily be generated using the `generate-silver-model` command from our own [dbt-chef](packages/dbt-chef/README.md) package (see instruction below). Intermediate and gold models require such an amount of transformations that these should be written from scratch. Hence, if creating an intermediate or gold model you need to create a sql.-file with the model name in the correct directory. I.e. if I am creating a product dimension in gold I will create a file called `dim_products.sql` under `models/gold/menu`. Please remember to follow the [naming conventions](#13-naming-conventions).

#### How to use the generate-silver-model command
The `generate-silver-model` command outputs a model file that gives you a head start of the development. Run the command below in your terminal and replace the following fields to create the model file.
- `<source_system>`: The source system the data belongs to (e.g. cms, pim etc)
- `<source_table_name>`: The name of the table in bronze
- `<model-name>`: The name of the model you are creating (please follow our [naming conventions](#13-naming-conventions))
  
```bash
dbt-chef generate-silver-model --source-system <source_system> --source-table-name <source_table_name> --model-name <model_name>
```

### b. Perform the needed transformations
There are different types of transformations that should be done depending on what layer you are creating the model in.

#### ℹ️ General code structure
Models in dbt are built using CTEs for each transformation step being done. 
* A model should always start with a CTE which does `select * from` the source/model of interest.
* After this there should be one CTE for each bigger transformation step with a name that describes the activity of the CTE, try to follow the format `table_of_interest_actions_done`, i.e., `product_tables_joined` or `deviations_filter_on_mealboxes`. The main purpose of the name is to make it easy to understand whats going on in the CTE for a random person.
* Lastly, the script should end by doing a `select * from` the last created CTE. Please review already created models to get a sense of this structure works in each layer.

The reason for using CTEs is to make the code more modular which in turn makes it easier to debug and reuse elements across the project. For more information about why dbt suggest to use CTEs, read [this article](https://docs.getdbt.com/terms/cte).

#### 🥈 Transformations in silver
The goal of the silver layer is to create the building block for the rest of the project. Hence, the main objective is to cleanse the source data and create a conform structre across the project.

The following transformations steps should be done in the silver layer:
- ✅ Renaming
- ✅ Type casting
- ✅ Basic computations (e.g. cents to dollars, add vat etc)
- ✅ Consistent casing of strings. Use initcap(), upper() or lower() to ensure consistent casing of strings where it make sense
- ✅ Remove column that are not relevant. We do not want to include columns from the source that will never be in use.

One should *not* do:
- ❌ Joins
- ❌ Unions
- ❌ Aggregations

Exceptions:
- If the silver layer has historic data or similar which comes from another source these should be combined in the silver layer and in this case unions or joins might be needed. In this case the legacy source data should be handeled in the `base`-folder

>[!NOTE]
>In silver we organize the columns based on data type. Please look at previously made silver model to see examples of this structure.

#### 🏄🏻‍♀️ Transformations in intermediate
The goal of the intermediate layer is to isolate out different concepts and calculations than can be reused across the gold layer or other intermediate tables. Below is examples of transformations that can be done in the intermediate layer. The intermediate tables will be populated as [ephemeral](https://docs.getdbt.com/docs/build/materializations#ephemeral) in test and prod, but as tables in dev to make it easier to debug. 

The following transformation steps can be done in the intermediate layer:
- ✅ Join silver tables to denormalise them
- ✅ Perform advanced calculations
- ✅ Pivot tables
- ✅ Filter data

#### 🥇 Transformations in gold
The goal of the gold layer is to create a dimensional data model that can be used for analyses purposes.

The following transformation steps should done in the gold layer:
- ✅ Join silver and/or intermediate tables
- ✅ Add primary keys
- ✅ Add foregin keys
  - Add foreign keys by concatenating columns and performing the needed transformations to create the key
  - Only when needed, join in the dim table to get extract foreign key
- ✅ Other needed calculations that does not beloing to the intermediate layer
  
One should *not* do:
- ❌ Join with other facts (then the needed logic should be moved to the intermediate layer)

>[!NOTE]
>Keep the id columns used to create foreign keys in the table as they are useful for QA or if doing ad hoc analysis where gold and silver tables are combined.

## 2.4 Add model configurations

Each subdirectory of the silver and gold layer has a .yml-file with the suffix `__models.yml`. All models in silver and gold need to be added to these files (not intermediate). Start by adding the following lines to the end of the `__models.yml`-file for the models you are creating in silver and/or gold. Later on we will add more configurations to the columns of the model as well. Read more about model configurations in dbt [here](https://docs.getdbt.com/reference/model-properties).

#### Silver
```yml
- name: model_name
  description: ""

  #column configs to be added here

```

#### Gold
```yml
- name: dim_dates
  description: ""
  latest_version: 1
  config:
    alias: dim_dates
    contract:
      enforced: true
    
  versions:
    - v: 1

  #column to be added here

```

## 2.5 Deploy model
When the transformations are done you can deploy the model to Databricks. Each developer has their own developer schema in the following format: `~firstname_lastname`, which is set when creating the [local profile for dbt](#41-set-up-local-dbt-profile). When deploying the models to Databricks this is where the tables will be populated. The developer schemas are described in further details [here](#3-working-with-developer-schemas)

>[!TIP]
>Here are some other commands that you may find useful:
>- `dbt run -s +model_name` to run a specific model and all the models that it depends on
>- `dbt run -s model_name+` to run a specific model and all the models that are depending on it
>
>Run `dbt` in the terminal to see all the other available commands. Or read more about the commands [in dbts docs](https://docs.getdbt.com/reference/dbt-commands).

## 2.6 Debugging
After you have deployed your model you should check that the output is as expected and debug if needed. Here are some tips for debugging:
- If you run `dbt compile -s model_name` in your terminal you will get outputted the compiled code which you can copy paste in to Databricks SQL Editor.
- In the dbt project there is a folder called `target` which has the compiled code stored under the subdirectory called `compiled` you can also copy paste code from there to Databricks SQL Editor.

>[!NOTE]
>The target folder will keep scripts from all models that have been run from your terminal. To clean this up you can simply just delete the folder as it will be regenerated next time you run `dbt compile`, `dbt run` or `dbt build`.

## 2.7 Add documentation
After you are finished with your model you need to add documentation.

### a) Adding documentation to tables 
Table description should be added directly under description in the `__models.yml`-file.

### b) Adding documentation to columns

Each subdirectory of the layers has a .md-file with the suffix `__docs.md`. This is used to store documentation for columns using [doc blocks](https://docs.getdbt.com/docs/build/documentation#using-docs-blocks). The purpose of this is to be able to reuse documentation across models.

When adding documentation please follow the instructions below:
1. Add a heading with the table name you are generating documentation for to the `__docs.md`-file
2. Run the `generate-docs` command from [dbt-chef](packages/dbt-chef/README.md) in the terminal. Swap out <model_name> with the name of the model you are generating documentation for.
```
dbt-chef generate-docs --model-name <model_name>
```
3. Copy the output which is printed in the terminal to the `__docs.md`-file under the heading you created in step 1.
4. Remove or relocate columns that originate from another table.
   - If a column originates from another table the doc block should be placed under that model name instead.
   - Remove the doc block if the documentation already exists.
   - Add the doc block under the right model name if it does not already exist.
6. Fields that are common across several source systems and does not have a clear source of origin should be added to `_common_docs.md`.
7. Write documentation for the remaining fields.
- Exceptions: The might be situations where one want to have different documentation for the same column, in that case one would add the table name after the column name in the doc block.

>[!NOTE]
>The documentation will be visible in the catalog in Databricks. You can also view the documentation you can run `dbt docs generate` followed by `dbt docs serve` in the terminal. Mark that for this to be possible the next step must be done first.

## 2.8 Add columns to model configurations
Each column should be configured with a description, [constraints](https://docs.getdbt.com/reference/resource-properties/constraints) and [data tests](https://docs.getdbt.com/docs/build/data-tests). Follow the instructions below to set up the configurations.
1. Run the generate-yaml from [dbt-chef](packages/dbt-chef/README.md) command in the terminal. Swap out <model_name> with the name of the model you are working with.
```
dbt-chef generate-yaml --model-name <model_name>
```
2. Copy output and add it to the `__models.yml`-file after `description:` if its in silver or before `versions:` if its in gold.
3. Remove the generated constraints and data tests that are not appropriate fro your model.
4. Add constraints and data tests that are needed.
5. Fill in accepted values when appropriate (see tip further down).
6. Run `dbt build -s model_name` (NB! this time we use `dbt build` and not `dbt run` as this will also run the tests we have added).
7. Now that you added constraints and tests these might fail. Debug and fix the errors if occuring.

>[!TIP]
> To generate accepted values easily you can do a select distinct on the field after builidng the model to your personal silver schema and then use ChatGPT to format it for you as a list

## 2.9 Create pull request
When everything works as it should you can create a pull request in GitHub and assign reviewers. When a pull request is created the models you have created will be moved to our the schemas in our developoment workspace. After the PR is merged it will be moved to the test workspace. After this it will be moved to the prod workspace when some of the admins approve the deployment.

>[!NOTE]
>Eventhough you should not create a pull request before the code are finished and tested you should still create commits and push when making changes. Pushing the code will make sure that its stored online and not only locally on your computer. Doing commits make it possible to go back in time in your code, i.e. everytime you do a commit you save a new version of your code which can be reverted back to later if needed.

>[!TIP]
>If you want to see how your code looks in Github, but are not ready to create a pull request yet you can create a [draft pull request](https://github.blog/news-insights/product-news/introducing-draft-pull-requests/)

# 3. Working with developer schemas
This sections explains in further detail about the develop schemas and how to work with them. Each developer should have their own schema in the Databricks Dev Workspace which their models are populated to when developing locally. The reason for this setup is to avoid conflicts between developers while developing features.

The development schema is set when creating the [local profile for dbt]((#4-setting-up-dbt-for-the-first-time)) and should be on the following format: `~firstname_lastname`. This will end up as a prefix to the schema of your models. I.e. if you deploy a silver model the schema of this model would be `~firstname_lastname_silver`, while for gold it will be `~firstname_lastname_gold`.

You are not supposed to have all the tables of the data model in your development schema, but rather those that are needed for what you are working on in the moment.

## 3.1 Insert tables from dev.silver to ~firstname_lastname_silver
When deploying models in dbt (i.e. when running `dbt run` or `dbt build` silver tables will be populated based on the data in the bronze layer. For some tables we have an incremental load, meaning that we only fetch a smaller amount of data at a time from the source system and insert it incrementally to silver. For these tables you will not get all the data when deploying the silver models, hence to be able to fetch all the data (if needed - often its not) we have created a job in Databricks that insert tables from dev.silver to your developer schema. Read instructiions below for how to use it.

1. Go to the the job called [dbt_developer_bulk_ingest_silver_tables](https://adb-4291784437205825.5.azuredatabricks.net/jobs/509482350039207?o=4291784437205825) in the Databricks Dev Workspace.
2. Select the arrow next to `run` up in the left corner and chose `run now with different parameters`
3. Fill in the parameters
   * `firstname_lastname`: Write your firstname and lastname (same as for the developer schema). Its not needed to add ~.
   * `table_list`: Leave empty if you want to ingest all tables or specify which tables you want to ingest in the following format: cms__companies, cms__billing_agreements.
4. Run the job

## 3.2 Bulk delete tables in developer schema
After having worked with dbt for a while the developer schema can become messy. Then its handy to be able to bulk delete tables from the schemas. Follow the steps below.
1. Go to the job called [dbt_developer_bulk_delete_tables](https://adb-4291784437205825.5.azuredatabricks.net/jobs/335350526735126?o=4291784437205825).
2. Select the arrow next to `run` up in the left corner and chose `run now with different parameters`
3. Fill in the parameters
   * `firstname_lastname`: Write your firstname and lastname (same as for the developer schema). Its not needed to add ~.
   * `layer`: Suffix of the schema you want to empty (i.e. silver, intermediate, gold or mlgold)
4. Run the job

## 3.3 Bulk delete all tables from developer schema
If you want to empty all your development schemas you can follow these steps:

1. Go to the job called [dbt_developer_schema_full_delete](https://adb-4291784437205825.5.azuredatabricks.net/jobs/479188651788245?o=4291784437205825)
2. Select the arrow next to `run` up in the left corner and chose `run now with different parameters`
3. Fill in the parameters
   * `firstname_lastname`: Write your firstname and lastname (same as for the developer schema). Its not needed to add ~.
3. Run the job

# 4 Setting up dbt for the first time
To get started developing in dbt follow the steps described in this section. This assumes that you have [git and sous-chef](https://github.com/cheffelo/sous-chef) set up in your local development environment (using Visual Studio Code or another IDE).

## 4.1 Set up local dbt profile
Your local dbt profile is used to conncet to Databricks from your local development environment. The connection details is stored in a yml-file for the following path on your computer: `[USERPATH]/.dbt/profiles.yml`. Please follow the instructions below to set up the profile.

1. Navigate to your userprofile-folder on your computer
2. View hidden folders by running using the following hot keys for Mac/Windows: `cmd+shift+dot / ???`
3. Create a folder called .dbt if you do not already have it
4. Create a file called profiles.yml inside the .dbt-folder
5. Open the file in vscode
6. Add the script below and change the files when needed:

```yml
 transform:
  target: local_dev
  outputs:
    local_dev:
      type: databricks
      catalog: dev
      schema: ~firstname_lastname # Need to be configured by you
      host: adb-4291784437205825.5.azuredatabricks.net
      http_path: /sql/1.0/warehouses/45100b61eb7ee2f5
      token: dapiXXXXXXXXXXXXXXXXXXXXXXX # Need to be configured configured by you
      threads: 4
```
Replace with the following:
- `schema`: The schema should be your firstname and lastname in the following format firstname_lastname.
- `token`: Follow [these instructions](https://docs.databricks.com/en/dev-tools/auth/pat.html#databricks-personal-access-tokens-for-workspace-users) to generate a personal access token.

>[!WARNING]
>Never store the token another place than in the .dbt/profiles.yml or in a safe key vault.

## 4.2 Test connection

1. Open sous-chef in your IDE (i.e. Visual Studio Code)
2. Enter the data-model project folder: `cd projects/data-model`
3. Activate the virtual environment: `poetry shell`
4. Install dependencies: `poetry install`
5. Enter the dbt-project: `cd transform`
6. Run `dbt debug` in the terminal

The output from running `dbt debug` should look something like this if the set up are done correctly. If all checks passes you are ready to start developing in dbt. 🥳

```shell
07:21:56  Running with dbt=1.8.3
07:21:56  dbt version: 1.8.3
07:21:56  python version: 3.11.7
07:21:56  python path: /Users/marie.borg/Library/Caches/pypoetry/virtualenvs/data-model-tSUiXESf-py3.11/bin/python
07:21:56  os info: macOS-14.5-arm64-arm-64bit
07:22:02  Using profiles dir at /Users/marie.borg/.dbt
07:22:02  Using profiles.yml file at /Users/marie.borg/.dbt/profiles.yml
07:22:02  Using dbt_project.yml file at /Users/marie.borg/Documents/Cheffelo/sous-chef/projects/data-model/transform/dbt_project.yml
07:22:02  adapter type: databricks
07:22:02  adapter version: 1.8.3
07:22:02  Configuration:
07:22:02    profiles.yml file [OK found and valid]
07:22:02    dbt_project.yml file [OK found and valid]
07:22:02  Required dependencies:
07:22:02   - git [OK found]

07:22:02  Connection:
07:22:02    host: adb-4291784437205825.5.azuredatabricks.net
07:22:02    http_path: /sql/1.0/warehouses/7a406a7f9665587e
07:22:02    catalog: dev
07:22:02    schema: marie_borg
07:22:02  Registered adapter: databricks=1.8.3
07:22:11    Connection test: [OK connection ok]

07:22:11  All checks passed!
```

## 4.3 Debugging
If you have any trouble the soultion might be here:

### Error: The Poetry configuration is invalid
```
The Poetry configuration is invalid:
  - Additional properties are not allowed ('package-mode' was unexpected)
```
If you get the above error you may need to update your local Poetry version, do this by running: `poetry self update`.

If you are using Windows, you may need to run the following command to install Poetry if the above command fails:
```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```
### Errors when running dbt debug
If you have errors during the debug, then restarting your machine may do the trick or creating a new access token
