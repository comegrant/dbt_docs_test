# About the project

This projects holds code for the ETL pipelines of the Data Platform. We use Databricks Notebooks for ingestion, dbt for transformations and Databricks Asset Bundles (DAB) for orchestration (workflows).

## Folder Structure

**data-model/ingest**: Code related to ingestion of source data

**data-model/resources**: Code for setting up workflows using DABs

**data-model/transform**: dbt project files

# Development setup
This section describes how you set up the local development environment.

## dbt
To get started developing in dbt follow the steps described in this section. This assumes that you have [git and sous-chef](https://github.com/cheffelo/sous-chef) set up in your local development environment (using Visual Studio Code or another IDE).

### 1. Set up sql warehouse
When developing in dbt you need to specify which compute to use when deploying your code to the development environment in Databricks. Hence, you need to create a SQL warehouse to be used.

To create a SQL Warehouse you need to go to [Compute > SQL warehouse > Create SQL warehouse](https://adb-4291784437205825.5.azuredatabricks.net/compute/sql-warehouses?o=4291784437205825&page=1&page_size=20).

Settings for the SQL warehouses should be the following:
* `Name`: First_name's dbt SQL Warehouse
* `Cluster size`: 2X-Small
* `Auto stop`: After 10 minutes
* `Scaling`: Min: 1 Max: 2
* `Type`: Serverless

Open advanced options to add these four tags which will be used to be able to track costs:
1. `Key`: tool, `Value`: dbt
2. `Key`: env, `Value`: dev
3. `Key`: user, `Value`: first name
4. `Key`: managed_by, `Value`: manually

In advanced options, also have the following settings:
* `Unity Catalog` : on
* `Channel`: Current

See example of set up here:
<p align="center">
<img src="../../assets/data-model/dbt-sql-warehouse-example.png" alt="dbt-sql-warehouse-example"/>
</p>

### 2. Set up local dbt profile
To connect dbt to Databricks you need to save connection details in a yml-file for the following path on your computer: `[USERPATH]/.dbt/profiles.yml`. This file should absolutely **not** be committed to the repository.

1. Go to youe userprofile-folder on your computer
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
      host: xyz.azuredatabricks.net # Need to be configured by you
      http_path: /SQL/YOUR/HTTP/PATH # Need to be configured by you
      token: dapiXXXXXXXXXXXXXXXXXXXXXXX # Need to be configured configured by you
      threads: 4
```
Replace with the following:
* `schema`: The schema should be your firstname and lastname in the following format firstname_lastname.
* `host`: Copy serverhost name under connection details of your SQL Warehouse.
* `http_path`: Copy HTTP path under connection details of your SQL Warehouse.
* `token`: Follow [these instructions](https://docs.databricks.com/en/dev-tools/auth/pat.html#databricks-personal-access-tokens-for-workspace-users) to generate a personal access token.

**NB!** You never should store the token another place than in the .dbt/profiles.yml.

### 3. Activate virtual environment
We use a virtual environment when working in sous-chef. This will ensure that you are using the right python version and package versions when developing. To activate the virtual environment open sous-chef in the code editor you prefer and follow the steps below in your terminal:

1. Enter the project folder in sous chef: `cd projects/data-model`
2. Activate the virtual environment: `poetry shell`
3. Install dependencies: `poetry install`

<details>
<summary>Read this if you get any errors</summary>

**Error: The Poetry configuration is invalid**
```
The Poetry configuration is invalid:
  - Additional properties are not allowed ('package-mode' was unexpected)
```
Then you may need to update your local Poetry version, do this by running: `poetry self update`

_If you are using Windows, you may need to run the following command to install Poetry if the above command fails:_
```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```

</details>

### 4. Enter the dbt project
Enter the dbt project by writng this in your terminal: `cd transform`

### 3. Check databricks connection
Run `dbt debug` in the terminal, the output should look something like this if the set up are done correctly:

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

> [!TIP]
> If you have errors during the debug, then restarting your machine may do the trick or creating a new access token

### 6. Start developing 🥳
Hurray! Now you can start developing in dbt.

Please ensure to follow the the guidelines in the [Data Model Development](#data-model-development) section.

# Data Model Development: Ingest
Ingest refers to fetching data from the source databases and other source systems. Each source system has a notebook for ingest under the ingest folder. Changes to ingestion can be done from the Databricks Dev Workspace or in Visual Studio Code.

<details>
<summary>Databricks Dev Workspace</summary> 
1. Go into sous chef from Databricks
2. Pull changes from main and create a new branch
3. Open the relevant ingest notebook (`bronze_<source_system>_full`)
4. Add the tables you want to include in the tables list in alphabetic order
5. Commit and push changes from Databricks
6. Create a PR and assign Marie or Anna to review

To ingest the data immediatly to bronze you can run the notebook only for the tables that you have added by either commenting out the other tables or copy the cell and remove the tables that already exist.

</details>

<details>
<summary>Visual Studio Code</summary> 
1. Ensure that you are in main: ```git checkout main```
2. Pull the latest changes from main: ```git pull```
3. Create a new branch: ```git checkout -b "firstname/your-branch-name"```
3. Open the relevant ingest notebook (`bronze_<source_system>_full`)
4. Add the tables you want to include in the tables list in alphabetic order
6. Commit and push changes to GitHub (ask if you do not how)
7. Create a PR and assign Marie or Anna to review
</details>

# Data Model Development: Transform
This section contains information on how to develop in our dbt project. `Models` refers to the script which does the transformation to the data, while `tables` refers to the end results which is found in Databricks as a table.

## Deployment and Developer Schema
Each developer should have their own schema in the Databricks Dev Workspace which their models are populated to when developing locally. The reason for this setup is to avoid conflicts between developers while developing features. This section explains how the developer schema setup works.

The development schema is set when creating the [local profile for dbt]((#2-first-time-only-set-up-local-dbt-profile)) and should be on the following format: `~firstname_lastname`. This will end up as a prefix to the schema of your models. I.e. if you deploy a silver model the schema of this model would be `~firstname_lastname_silver`, while for gold it will be `~firstname_lastname_gold`.

#### Deploy Models
You can deploy dbt models from the CLI by using any of the following commands. You can read more about [dbt CLI commands here](https://docs.getdbt.com/reference/dbt-commands). 

Here are some commands that you may find useful:
- `dbt run -s model_name` to run a specific model
- `dbt run -s +model_name` to run a specific model and all the models that it depends on
- `dbt run -s model_name+` to run a specific model and all the models that are depending on it
- `dbt build -s model_name` to compile, run and test a specific model all at once (can you + in the same way as with run)
- `dbt test -s model_name` to run tests for a specific model

### Insert tables from dev.silver to ~firstname_lastname_silver
Data can be inserted to `~firstname_lastname_silver` in two ways:
1. By deploying a silver model through the CLI
2. By running the job called [dbt_developer_bulk_ingest_silver_tables](https://adb-4291784437205825.5.azuredatabricks.net/jobs/509482350039207?o=4291784437205825) in the Databricks Dev Workspace.
  a. Select the arrow next to "run now with different parameters"
  b. Fill in the parameters. If you want to ingest all the tables from dev.silver leave the `table_list` empty else fill in the tables you want to load in the following format: cms__companies, cms__billing_agreements.
  c. Run the job
Some of the models in silver is incremental, meaning that the data in bronze is not complete. If you need complete data you must ingest data to silver by using option 2.

### Bulk delete tables in ~firstname_lastname schemas
If you want to delete all the tables in one of your `~firstname_lastname` schemas you can use the job called [dbt_developer_bulk_delete_tables](https://adb-4291784437205825.5.azuredatabricks.net/jobs/335350526735126?o=4291784437205825).
1. Select the arrow next to "run now" adn chose "run now with different parameters"
2. Fill in the parameters. The field `layer` should contain the suffix of the schema you want to empty.

### Full Refresh of Development Schema
If you want to delete tables in all the `~firstname_lastname`-schemas you can use the job called [dbt_developer_schema_full_delete](https://adb-4291784437205825.5.azuredatabricks.net/jobs/479188651788245?o=4291784437205825)
1. Select the arrow next to "run now" adn chose "run now with different parameters"
2. Fill in the parameters
3. Run the job

## Project layers, Subdirectories and Model Names

### Silver 🥈
⭐️ **Purpose:** Contains raw tables which have been cleansed and standarized.
📁 **Subdirectories:** Models are divided into folders using the source name as folder name (e.g. cms, pim, product_layer, ops, segment++)
📃 **Model names:** `<source_system>__<source_table_name>(s).sql`
- All tables that ends with the entity of the table should be end in plural, e.g billing_agreement_order_line contains several order lines, hence the appropriate name is billing_agreement_order_lines
- If the table is a bridge table it should be plural for both entites that are being linked, otherwise only the last word in the table should be plural.
- There are some edge cases where the table name should not be plural, this is for instance if the table name is something else than the entity of the table. E.g. tables that end with legend should not be plural as it does not contains several legend but is a legend for some type of item.

### Intermediate 🏄🏻
⭐️ **Purpose:** Performs need transformations to silver tables need before entering the gold layer.
📁 **Subdirectories:** Models are divided using business groupings as folder names (e.g. common, sales, menu, marketing, operations ++)
📃 **Model names:** `int_<silver_model>_<verb>s.sql`
- The file name should describe the table being transformed and the transformation being done
- Example: `int_billing_agreement_addon_subscriptions_pivoted` or `int_billing_agreements_extract_first_order`.

### Gold 🥇
⭐️ **Purpose:** Join together models from silver and intermediate to create a data model which follow the principles of dimensional data modelling.
📁 **Subdirectories:** Models are divided using business groupings as folder names (e.g. common, sales, menu, marketing, operations ++)
📃 **Model names:** `int_<silver_model>_<verb>s.sql`, e.g, `int_billing_agreement_addon_subscriptions_pivoted` or `int_billing_agreements_extract_first_order`.
- The model file should start with fact or dim based on the type of table followed by a logic business related name in plain english
- One should NOT create models with the same concept for several teams. I.e., there should not be a table for `finance_orders` and `marketing_orders`.

## Best Practice

### Naming conventions

- 🐍 Use snake_case
- 🚫 Avoid using reserved words (such as [these](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-reserved-words) for Azure Databricks)
- 📅 Date should be named as `<event>_date`, e.g. `created_date`
- ⏱️ Timestamp should be names as `<event>_at`, e.g. `created_at`
- 🔙 Events dates and times should be past tense — created, updated, or deleted.
- 🎚️ Booleans should be prefixed with `is_` or `has_` etc., e.g., `is_active_customer` and `has_admin_access`
- 🔑 Id columns that are used as primary keys in the source system should always be called `<table_name>_id` e.g `billing_agreement_id`
- 💪 Consistency is key! Use the same field names across models where possible. For example, an id to the `billing_agreement` table should be named `billing_agreement_id` everwhere rather than alternating with `customer_id`

### General rules SQL
- Use lower case for all code
- Use leading commas
- Do not use abbreviation in aliases and CTEs, i.e, use `order_lines` rather than `baol`
- Joins should always be done using left join and a where clause if filtering is needed instead of an inner join
- Grouping (and ordering) should be done implisitt by either using `group by 1, 2 ,3 etc` or `group by all`. See this [doc](https://www.getdbt.com/blog/write-better-sql-a-defense-of-group-by-1) for reference on why.

#### Code structure
Each model should start with a CTE which does `select * from` the source/model of interest. In silver you would typically call this CTE `source` since you only extract from one table, while in intermediate and gold whereby you join several models you typically give the CTEs names which referes to the model it reads from. After this there should be one CTE for each bigger transformation step which describes the activity of the CTE. Lastly, the script should end by doing a `select * from` the last created CTE. See the pseudo code below or look into already created models to checkout the structure.

```
with

source/model_name as (

    select * from source/model

),

main_activity_1 as (

    you're query

)

main_activity_2 as (

    you're query

)

select * from main_activity_2
```

#### Why dbt recommend using CTEs
The reason for using CTEs is to make the code more modular which in turn makes it easier to debug and reuse elements across the project.

For more information about why dbt suggest to use CTEs, read [this](https://docs.getdbt.com/terms/cte).

## Modelling in Silver

### 1. Add model to `_<source_system>__sources.yml`
When adding new tables to the silver layer you first have to add the table name in bronze to the `_<source_system>_source.yml` file. This ensure that one can refer to it in when creating the model by using the [source()-function](https://docs.getdbt.com/reference/dbt-jinja-functions/source)

### 2. Create model
*Coming...* evaluate if table need snapshot

Create the model file in the right folder and start to clean the data.

To get a head start you can use `generate-silver-model` from our own [dbt-chef](packages/dbt-chef/README.md) package to output a file with all the source columns by running the following command in your terminal:

```bash
dbt-chef generate-silver-model --source-system <source_system> --source-table-name <source_table_name> --model-name <model_name>
```

For example:
```bash
dbt-chef generate-silver-model --source-system cms --source-table-name cms__company --model-name cms__companies
```

#### Transformations in the silver layer
After the model script is created its time to start transforming the model. Please follow these steps:
1. Remove columns from the file which is not relevant for the model. We do not want to include columns from the source that will never be in use.
2. Place the columns under the right data type grouping
3. Perform relevant transformations (see instruction below)

The most standard transformations steps in the silver layer:
- ✅ Renaming
- ✅ Type casting
- ✅ Basic computations (e.g. cents to dollars, add vat etc)
- ✅ Consistent casing of strings. Use initcap(), upper() or lower() to ensure consistent casing of strings where it make sense.

One should *not* do:
- ❌ Joins — the goal of staging models is to clean and prepare individual source-conformed concepts for downstream usage. We're creating the most useful version of a source system table, which we can use as a new modular component for our project. In our experience, joins are almost always a bad idea here — they create immediate duplicated computation and confusing relationships that ripple downstream — there are occasionally exceptions though.
- ❌ Aggregations — aggregations entail grouping, and we're not doing that at this stage. Remember - staging models are your place to create the building blocks you’ll use all throughout the rest of your project — if we start changing the grain of our tables by grouping in this layer, we’ll lose access to source data that we’ll likely need at some point. We just want to get our individual concepts cleaned and ready for use, and will handle aggregating values downstream.

#### Base models in the silver layer
In some situations one need to do joins or unioning too make a source table complete. This could be if there is a separate delete tables that holds information about which customers that are deleted (joining) or if there are history table holding historical records (unioning). In these cases the source tables should be placed in the the base folder and then create a final table by joining/unioning which is stored with the other final models.

### 3. Add model to `_<source_system>__models.yml`
After creating the model you need to add the code below to the `_<source_system>__models.yml`.

```yml
 - name: model_name
   description: ""
```

### 4. Add documentation to silver models
Add documentation to the created models and used source.

**Source:** Source description should be added directly under description in `_<source_system>__source.yml`.

**Tables:** Table description should be added directly under description in `_<source_system>__models.yml`.

#### Columns
Descriptions of columns should be added to the `_<source_system>__docs.md.
1. Add a heading with the table name you are generating documentation for to `_<source_system>__docs.md`
2. Run the `generate-docs` command from [dbt-chef](packages/dbt-chef/README.md) in the terminal:
```
dbt-chef generate-docs --model-name <model_name>
```

For example:
```
dbt-chef generate-docs --model-name cms__companies
```

3. Copy the output to `_<source_system>__docs.md` under the table name heading
4. Remove columns that does not originate from the table: 
    *The script output doc blocks for all columns in the model, however you should only include descriptions of columns that originates from that table, meaning that for instance ids that originates from another table should be described under that table heading. Fields that are common across several source systems and does not have a clear source origin should be added to `_common_docs.md`.*
5. Write documentation for the fields and ensure to include the following:
  * When the table gets populated if its at a specific time (e.g. order gen).
  * Information about when and how the table rows gets updated.

#### Viewing documentation
To view the documentation you can run `dbt docs generate` followed by `dbt docs serve` in the terminal.

### 5. Add columns to `_<source_system>__models.yml`
After creating the documentation of the columns you need to refer to it in `_<source_system>__models.yml` as well.
1. Run the generate-yaml from [dbt-chef](packages/dbt-chef/README.md) command in the terminal:
```
dbt-chef generate-yaml --model-name <model_name>
```

For example:
```
dbt-chef generate-yaml --model-name cms__companies
```

2. Copy output and add it after description in `_<source_system>__models.yml`

### 6. Add tests to silver models
The [generate_model_yaml](transform/macros/code-generation/generate_model_yaml.sql) macro adds some default tests automatically to the columns. However these are just made based on assumptions and must be updated for each column to be the correct type of test. Furthermore one need to create other tests as well if reasonable. Follow the steps below:
1. Remove automatics generated test that are not relevant
2. Fill in accepted values for fields where it's relevant
3. Add more data tests if relevant, read more about [data tests](https://docs.getdbt.com/docs/build/data-tests) in dbt here.

>[!TIP]
> To generate accepted values easily you can do a select distinct on the field after builidng the model to your personal silver schema and then use ChatGPT to format it for you as a list

### 7. Deploy silver model
[Deploy](#deployment-of-models) the model and check if the result is as expected in Databricks under your own silver schema.

## Modelling in Intermediate and Gold
The gold layer consist of models that are optimized for reporting. Before creating the model in Gold you should deploy the relevant models in Silver if you have not done this yet to avoid errors while developing.

### 1. Create intermediate models if necessary
To make transformation logic as modular as possible we make use of intermediate models. If you need to do major transformations to a table before joining it with other tables in the gold layer it should have an intermediate model. Antoher reason for using intermediate models is if there are several gold models that need to reuse the same logic. The intermediate models should be place in the intermediate folder under the correct business concept. They will be populated as [ephemeral](https://docs.getdbt.com/docs/build/materializations#ephemeral) in test and prod, but as tables in dev to make it easier to debug.
* Do the transformations needed to get the wanted result
* Use CTEs for each transformation step to make the code modular just like in silver

### 2. Create gold models
The models in the gold layer can be put together by combining models from silver and intermediate.
* Do the transformations needed to get the wanted result
* Use CTEs for each transformation step to make the code modular just like in silver and intermediate.
* Add primary keys to the table called `pk_<dim>_<tablename>`. This should be created by creating an hash by concatenating the columns needed for it to be unique by using the md5() function and concatenate. None of the columns used for the pk should contain null values.
* Add foreign keys if creating a fact table in the same way as the primary keys are created.

### 3. Add gold models to `_<business_concept>__models.yml`
After creating the gold model you need to add the code below to the `_<business_concept>__models.yml`.

```yml
  - name: dim_date
    description: ""
    latest_version: 1
    config:
      alias: dim_date
    
    columns:
    
    versions:
      - v: 1
```

### 5. Add documentation of gold models

#### Tables
Table description should be added directly under description `_<business_concept>__models.yml`.

#### Columns
All columns coming from the silver layer should already be documented there and the documentation hence do not need to be added. All new columns that have been created should be added to `_<business_concept>_docs.md` following the steps below:
1. Add a heading with the table name to `_<business_concept>__docs.md`
2. Add columns that are not documented yet

### 6. Add columns to model.yml
After creating the documentation of the columns you need to refer to it to `_<business_concept>__models.yml` as well.
1. Run the generate-yaml from [dbt-chef](packages/dbt-chef/README.md) command in the terminal:
```
dbt-chef generate-yaml --model-name <model_name>
```

For example:
```
dbt-chef generate-yaml --model-name dim_companies
```
2. Copy output and add it between `columns:` in `_<business_concept>__models.yml`

### 7. Add tests to gold models
*Coming...* constraints

Follow the same steps as in [silver](#6-add-tests-to-silver-models) to add tests to the gold models.

### 8. Deploy intermediate and gold models
[Deploy](#deployment-of-models) the model and check if the result is as expected in Databricks under your own intermediate and gold schema.

## Deployment of models
After finishing a model in your local development enviroment you should deploy it to the Databricks Dev Workspace and check that the result is as expected. If the changes are as expected you can create a pull request.

### 1. Deploy changes from local environment
To assess that changes made has the expected output you need to deploy your changes to Databricks. To do this you can wrtie the following in your terminal.
1. Deploy changes to Databricks Dev Workspace: `dbt build -s +model_filename`

The deployed changes will end up under your own silver and gold schemas in Databricks which is indentified by having your firstname and lastname as prefix.

> [!TIP]
> Run `dbt` in the terminal to see all the other available commands. Or read more about the commands [in dbts docs](https://docs.getdbt.com/reference/dbt-commands).

### 2. Create pull request
If the changes are as expected and you are happy with your work please create a pull request for Anna and/or Marie to review.

## Debugging

### Target folder
The code you create will be translated to the right syntax for Databricks. The compiled code can be found in the target folder under `compiled` and the code that is run in Databricks can be found under `run`. This can be useful to look at if you experience some troubles with your code.

You can run `dbt compile` in the terminal to just compile the code with out deploying to Databricks to look at how it will turn out.

The target folder will keep scripts from models you have deleted. To clean this up you can simply just delete the folder as it will be regenerated next time you run `dbt compile` or `dbt build`, or you can run `dbt clean` which will also delete the folder until next time `compile`or `build` is run.
