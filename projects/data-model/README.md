# About the project

This projects holds code for the ETL pipelines of the Data Platform.

We use Databricks Notebooks for ingestion, dbt for transformations and Databricks Asset Bundles (DAB) for orchestration (workflows).

## Folder Structure

**data-model/ingest**: Code related to ingestion of source data

**data-model/resources**: Code for setting up workflows

**data-model/transform**: dbt project files

# Development setup
This section describes how you set up the local development environment.

## DABs - Workflow orchestration
To be written...

## dbt
To get started developing in dbt follow the steps described in this section. This assumes that you have git and [sous-chef](https://github.com/cheffelo/sous-chef) set up in your local development environment (using Visual Studio Code or something similar).

### 1. [First time only] Set up sql warehouse
When developing in dbt you need to specify which compute to use when deploying your code to the development environment in Databricks. Hence, you need to create a SQL warehouse to be used.

To create a SQL Warehouse you need to go to [Compute > SQL warehouse > Create SQL warehouse](https://adb-4291784437205825.5.azuredatabricks.net/compute/sql-warehouses?o=4291784437205825&page=1&page_size=20).

Settings for the SQL warehouses should be the following:
* `Name`: <Name>'s dbt SQL Warehouse
* `Cluster size`: 2X-Small
* `Auto stop`: After 10 minutes

Open advanced options to add these four tags which will be used to be able to track costs
1. `Key`: tool, `Value`: dbt 
1. `Key`: env, `Value`: dev 
1. `Key`: user, `Value`: marie
1. `Key`: managed_by, `Value`: manually  

See example of set up here:
<p align="center">
<img src="../../assets/data-model/dbt-sql-warehouse-example.png" alt="dbt-sql-warehouse-example"/>
</p>

### 2. [First time only] Set up local dbt profile
To connect dbt to Databricks you need to save credentials in a yml-file for the following path on your computer: `[USERPATH]/.dbt/profiles.yml`.

You should copy the template below and fill in the following:

* `schema`: The schema should be your firstname and lastname in the following format firstname_lastname.
* `host`: Copy serverhost name under connection details of your SQL Warehouse.
* `http_path`: Copy HTTP path under connection details of your SQL Warehouse.
* `token`: Follow [these instructions](https://docs.databricks.com/en/dev-tools/auth/pat.html#databricks-personal-access-tokens-for-workspace-users) to generate a personal access token.

**NB!** You never should store the token another place than in the .dbt/profiles.yml.

```yml
 transform:
  target: local_dev
  outputs:
    local_dev:
      type: databricks
      catalog: dev
      schema: firstname_lastname # Need to be configuest bu you
      host: xyz.azuredatabricks.net # Need to be configured by you
      http_path: /SQL/YOUR/HTTP/PATH # Need to be configured configured by you
      token: dapiXXXXXXXXXXXXXXXXXXXXXXX # Need to be configured configured by you
      threads: 4
```

### 3. Activate virtual environment
When working in dbt you use a virtual environment this will ensure that you are using the right python version and package versions when developing. All the commands written in the instruction below should be run from your terminal.

#### Windows
* Install docker: https://docs.docker.com/desktop/install/windows-install/
* Enter the project folder in sous chef: `cd projects/data-models`
* Spin up: `docker-compose run -it dev bash`

#### Mac
* Enter the project folder in sous chef: `cd projects/data-models`
* Activate the virtual environment: `poetry shell`
* Install dependencies: `poetry install`

### 4. Enter the dbt project
Enter the dbt project by writng this in your terminal: `cd transform`

### 5. [First time only] Check dbt connection
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

### 6. Create development branch
When making changes to the project you must pull the new changes in main and create a new branch using git. It's best practice to create branches for each type of change that you do. I.e., if you are adding data related to loyalty this can be one branch, while you should create with data for another domain which are not related to the loyalty tables. An example of a branch could be `marie/add-loyalty-ledger-to-silver`.

Write the following in your terminal when starting on a new feature:
* Pull last changes from main: `git pull`
* Create new branch: `git checkout -b "firstname/short-description-of-what-you-are-doing"`

### 7. Start developing 🥳
Hurray! Now you can start developing. 

Make sure to follow the guidelines in the [Data Model Deveopment](#data-model-development) section.

### 8. Deploy changes from local environment
To assess that change made has the expected output you need to deploy your changes to Databricks. Write the following in your terminal:
1. Install dbt packages: `dbt deps`
2. Deploy changes to Databricks Dev Workspace: `dbt build -s +model_filename` 
Run `dbt` in the terminal to see all the other available commands. Or read more about the commands [in dbts docs](https://docs.getdbt.com/reference/dbt-commands).

When deploying your changes from your local development environment these will end up under your own silver and gold schemas in Databricks which is detected by having your firstname and lastname as prefix.

### 9. Check that result is as expected
To check that results was as expected you can run queries inside Databricks on the data you've added.

### 10. Add, commit and push changes
While doing changes during development you should add and commit you changes for each step taken towards the final result. Steps could for instance be: Adding a table to silver, adding documentation, fixing a bug etc. This is basically a bit more advanced version of saving the changes.

The process is as follows:
* `git status`: list all the files that have been changed
* `git add [filename]`: add the files you want to commit
* `git commit -m "short description of what you did"`: Add a comment to what you did
* `git push`: Sends the changes to our github repo (doesn't have to be done before you are done)

First time you push from a branch you will get an error and something like below will show up. In this case just copy the line `git push --set-upstream origin marie/update-readme` and run this.

```
marie.borg@macM56CDVH4VT data-model % git push
fatal: The current branch marie/update-readme has no upstream branch.
To push the current branch and set the remote as upstream, use

    git push --set-upstream origin marie/update-readme

To have this happen automatically for branches without a tracking
upstream, see 'push.autoSetupRemote' in 'git help config'.
```

### 10. Create pull request
When you are finished developing on the branch you can merge you changes into main. This is done through creating a pull request. You can do that from the [GitHub UI](https://github.com/cheffelo/sous-chef).

# Development of workflows

Coming...

# Data Model Development: Ingest
Changes to ingestion can be done from the Databricks UI. Each source system has a notebook for ingest under the ingest folder. 
1. Go into sous chef from Databricks
2. Pull changes and create a new branch
3. Open the relevant ingest notebook (bronze_<source>_full)
4. Add the tables you want to include in the tables list 
5. Commit and push changes from Databricks and create a PR and assign Marie or Anna to review

To ingest the data immediatly to bronze you can run the notebook only for the tables that you have added by either commenting out the other tables or copy the cell and remove the tables that already exist. You can also run the whole notebook, it will not take that much time yet.

# Data Model Development: Transform
This section contains information and best practices related to data modelling in dbt. *Models* refers to the script which does the transformation to the data, while *tables* refers to the end product which is found in Databricks.

## Project structure

🥈 **Silver**: Contains models which clean and standardized data.  

🏄🏻 **Intermediate**: Intermediate models to break down transformations towards the gold layer.

🥇 **Gold**: Contains models which combine or heavily transform data. 

🛠️ **Utilities**: General purpose models that we generate from macros or based on seeds that provide tools to help us do our modeling, rather than data to model itself. Could for instance be a date table. (Not in use yet).

## Modelling in Silver
The silver layer consist of models that are cleansed and standardized. The models are based on tables from the bronze layer in the database. No joins should be done in this layer unless there are very special cases which is explained further under [base models](#7).

### 1. Add model to `_<sourcesystem>_source.yml`
When adding new tables to the silver layer you first have to add the table name in bronze to the `_<sourcesystem>_source.yml` file. This ensure that one can refer to it in when creating the model by using the [source()-function](https://docs.getdbt.com/reference/dbt-jinja-functions/source)

### 2. Create model

#### 1. Find the correct folder
Each source has its own subdirectory in the silver folder where the models are placed. In addtion there is a base folder which is used for models that are joined or unioned with another main model. This should only be done if absolutly needed. For instance if having tables containing historic data.

#### 2. Create model file
The model file name should follow this naming convention: `[sourcesystem]__[source_table_name](s).sql`. (E.g, cms__companies, or cms__billing_agreement_order_lines)

#### 3. Generate code
To get a head start you can generate a model script base by running the following code in your terminal, replacing `source_name` and `table_name` with the corresponding found in the `_[sourcesystem]__source.yml` file:

```shell
dbt run-operation generate_base_model --args '{
  "source_name": "cms", 
  "table_name": "cms__billing_agreement_order",
  "leading_commas": "true"
  }'
```

This will output code to the terminal that you can copypaste to the model-file.

#### 4. Organize code
* There should be CTEs for each bigger cleaning step.
* The CTEs should be named by the main acitivty in them.
* The columns should be organized in categories by data types just as in the example.
* Do not include columns from the source which is not needed

See example code below for reference. The `{{ source('source_name', 'table_name')}}` refers to the values in `_<sourcesystem>_source.yml`.
```sql

with

source as (

    select * from {{ source('stripe','payment') }}

),

renamed as (

    select
        -- ids
        id as payment_id,
        orderid as order_id,

        -- strings
        paymentmethod as payment_method,
        case
            when payment_method in ('stripe', 'paypal', 'credit_card', 'gift_card') then 'credit'
            else 'cash'
        end as payment_type,
        status,

        -- numerics
        amount as amount_cents,
        amount / 100.0 as amount,

        -- booleans
        case
            when status = 'successful' then true
            else false
        end as is_completed_payment,

        -- dates
        date_trunc('day', created) as created_date,

        -- timestamps
        created::timestamp_ltz as created_at

    from source

)

select * from renamed
```

#### 5. Transformations
The most standard transformations steps in the silver layer:
- ✅ Renaming
- ✅ Type casting
- ✅ Basic computations (e.g. cents to dollars, add vat etc)
- ✅ Consistent casing of strings. Use initcap(), upper() or lower() to ensure consistent casing of strings where it make sense.

One should *not* do:
- ❌ Joins — the goal of staging models is to clean and prepare individual source-conformed concepts for downstream usage. We're creating the most useful version of a source system table, which we can use as a new modular component for our project. In our experience, joins are almost always a bad idea here — they create immediate duplicated computation and confusing relationships that ripple downstream — there are occasionally exceptions though.
- ❌ Aggregations — aggregations entail grouping, and we're not doing that at this stage. Remember - staging models are your place to create the building blocks you’ll use all throughout the rest of your project — if we start changing the grain of our tables by grouping in this layer, we’ll lose access to source data that we’ll likely need at some point. We just want to get our individual concepts cleaned and ready for use, and will handle aggregating values downstream.

#### 6. Naming conventions

**General:**
- 🐍 Use snake_case
- 🚫 Avoid using reserved words (such as [these](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-reserved-words) for Azure Databricks)
- 💪 Consistency is key! Use the same field names across models where possible. For example, an id to the `billing_agreement` table should be named `billing_agreement_id` rather than `customer_id`

**CTEs/Table Alias**
- 🧐 Limit the use of abbreviations to improve readability. Use `order_lines` rather than `baol` for CTEs and table aliases.

**Tables**
* 💾 Table names should start with the source name followed by two underscores.
* 👯‍♀️ Table names should be in plural.

E.g, `cms__billing_agreement_order_lines` not `cms__billing_agreement_order_line`.

**Columns**
  * 🐍 Use snake_case
  * 📅 Date should be named as `<event>_date`, e.g. `created_date`
  * ⏱️ Timestamp should be names as `<event>_at`, e.g. `created_at`
  * 🔙 Events dates and times should be past tense — created, updated, or deleted.
  * 🎚️ Booleans should be prefixed with `is_` or `has_` etc., e.g., `is_active_customer` and `has_admin_access`
  * 💵 Price/revenue fields should have decimals (for example, `19.99` for 19.99kr)
  * 🔑 Id columns that are used as primary keys in the source system should always be called `[table_entity]_id` e.g `billing_agreement_id`
  
#### 7. Base models
In some situations one need to do joins or unioning too make a source table complete. This could be if there is a separate delete tables that holds information about which customers that are deleted (joining) or if there are history table holding historical records (unioning). In these cases the source tables should be placed in the the base folder and then create a final table by joining/unioning which is stored with the other final models.

### 3. Add model to models.yml
After creating the model you need to add it to the `_<sourcesystem>__models.yml`. Follow the format below and generate the code for the columns by using the [generate_model_yaml](transform/macros/code-generation/generate_model_yaml.sql) macro.

```yml

 - name: model_name
    description: ""

    columns:
    
    # Insert generated fields

```

#### Add tests
Test are automatically added if you use. However these are just made based on assumptions and must be updated for each column to be the correct type of test. Furthermore one need to create custom test as well if reasonable. 


### 4. Add documentation
Add documentation to the created models and used source. For columns we reuse documentation across the layers so check if the column is already documented before adding it.

#### Source
Source description should be added directly under description in _[sourcesystem]__source.yml.

#### Tables 
Table description should be added directly under description _[sourcesystem]__models.yml.

#### Columns
Column descriptions should be added to the _[sourcesystem]__docs.md and refered to in the _[sourcesystem]_models.yml files. You should add a heading with the table name and then add the doc blocks.

The name of the doc block for columns should be on the following format: column__[column_name]. E.g. `column__billing_agreement_id` or `column__product_variation_name`.

Use the [generate_column_docs](transform/macros/code-generation/generate_column_docs.sql) macro to get generate doc blocks for all the columns of a model.

This script output doc blocks for all columns in the model, however you should only include descriptions of columns that originates from that table, meaning that for instance ids that originates from another table should be described under that table heading. Field that are common across several source systems and does not have a clear source origin should be added to _common_docs.md. In other words you might need to more or remove some of the created doc blocks.

When documenting columns ensure to add the following if relevant:
* When the table gets populated if its at a specific time (e.g. order gen)
* Information about when and how the table rows gets updated

#### Viewing documentation
To view the documentation you can run `dbt docs generate` followed by `dbt docs serve` in the terminal.

### 6. Deploy
Deploy the model added by running `dbt build -s model_filename` in the terminal.

## Modelling in the Gold layer
The gold layer consist of models that are optimized for reporting. Before create the model in Gold you should deploy the relevant models in Silver if you have not done this yet.

### 1. Create intermediate models if necessary
To make transformation logic as modular as possible we make use of intermediate models. If you need to do major transformations to a table before joining it with other tables in the gold layer it should have an intermediate model. The intermediate models are created in the intermediate folder. They will be populated as CTEs in test and prod, but as tables in dev to make it easier to debug.

#### 1. Find correct folders
Both the intermediate and gold and subdirectories are based on business groupings.

#### 2. Create model file
For the intermediate models the file name should describe the transformation output: `int_[source_model]_[verb]s.sql`. E.g, `int_billing_agreement_addon_subscriptions_pivoted` or `int_billing_agreements_extract_first_order`.

#### 3. Organize code in CTEs
Each step of the transformation should be organized in CTEs with names that are describing the transformation being done. This makes it easier to debug the code and make changes to seperate parts of the code. See example below.

```sql

{%- set payment_methods = ['bank_transfer','credit_card','coupon','gift_card'] -%}

with

payments as (

   select * from {{ ref('stg_stripe__payments') }}

),

pivot_and_aggregate_payments_to_order_grain as (

   select
      order_id,
      {% for payment_method in payment_methods -%}

         sum(
            case
               when payment_method = '{{ payment_method }}' and
                    status = 'success'
               then amount
               else 0
            end
         ) as {{ payment_method }}_amount,

      {%- endfor %}
      sum(case when status = 'success' then amount end) as total_amount

   from payments

   group by 1

)

select * from pivot_and_aggregate_payments_to_order_grain
```

### 3. Gold layer
The models in the gold layer can be put together by combining models from silver and intermediate. 

#### 1. Find correct folders
Subdirectories are based on business groupings.

#### 2. Create model file
The model file should start with fact or dim based on the type of table followed by a logic business related name in plain english: `[fact/dim]_[business_name]s.sql`. E.g, `fact_orders` or `dim_billing_agreements`. One should NOT create models with the same concept for several teams. I.e., there should not be a table for `finance_orders` and `marketing_orders`.

#### 3. Create CTEs for further transformations and add PK and FKs
* Do the transformations needed to get the wanted result
* Use ctes to make the code modular and readable just like in silver and intermediate. 
* All tables should have a primary key called `pk_<dim>_<tablename>`. This should be created by creating an hash by concatenating the columns needed for it to be unique by using the md5() function and concatenate. None of the columns used should contain null values.
* If you are creating a fact table you need to make sure to add the relevant foreign keys for the table.

#### 4. Add model to model.yml
Coming...

#### 5. Add documentation
Coming...

#### 6. Add tests
Coming...

#### 7. Deploy model
Deploy the models by running `dbt deps` to install packages and `dbt build -s +model_filename` to deploy the specific model to databricks. The `+`-sign will ensure that all tables needed to create the table you want to check is run.

### Why dbt recommend using CTEs

For more information about why we use so many CTEs, read [this glossary entry](https://docs.getdbt.com/terms/cte).

- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as verbose as needed to convey what they do.
- CTEs with confusing or noteable logic should be commented with SQL comments as you would with any complex functions and should be located above the CTE.
- CTEs duplicated across models should be pulled out and created as their own models.

## Debugging

### Target folder
The code you create will be translated to the right syntax for Databricks. The compiled code can be found in the target folder under `compiled` and the code that is run in Databricks can be found under `run`. This can be useful to look at if you experience some troubles with your code. 

You can run `dbt compile` in the terminal to just compile the code with out deploying to Databricks to look at how it will turn out. 

The target folder will keep scripts from models you have deleted. To clean this up you can simply just delete the folder as it will be regenerated next time you run `dbt compile` or `dbt build`, or you can run `dbt clean` which will also delete the folder until next time `compile`or `build` is run. 


# DBT packages and macros
Coming ...