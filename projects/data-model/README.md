# About the project

This projects holds code for the ETL pipelines of the Data Platform.

It use Databricks Notebooks for ingestion, dbt for transformations and Databricks Asset Bundles (DAB) for orchestration (workflows). 
 

## Folder Structure

**data-model/ingest**: Databricks notebooks for ingestion of source data

**data-model/resources**: DAB Workflows

**data-model/transform**: dbt project

# Development setup
This section describes how you set up the local development environment.

## DABs

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

... not finished

## dbt
To get started developing in dbt follow these steps. This section assumes that you have git and [sous-chef](https://github.com/cheffelo/sous-chef) set up in your local development environment (using Visual Studio Code or something similar).

### 1. [Firste time only] Set up sql warehouse
When developing in dbt you need to specify which compute to use when deploying your code to the development environment. Hence, you need to create a SQL warehouse to be used.

To create a SQL Warehouse you need to go to [Compute > SQL warehouse > Create SQL warehouse](https://adb-4291784437205825.5.azuredatabricks.net/compute/sql-warehouses?o=4291784437205825&page=1&page_size=20). The cluster should follow the same naming conventions as outlined [here](https://chef.getoutline.com/doc/compute-hJMBQnxLb3).

### 2. [First time only] Set up local dbt profile
To connect dbt to Databricks you need to save credentials in a yml file for the following path on your computer: `[USERPATH]/.dbt/profiles.yml`.

You should copy the template below and fill in the following:

* `schema`: The schema should be your firstname and lastname in the following format firstname_lastname.
* `host`: Go to the SQL Warehouse you created and to connection details copy serverhost name.
* `http_path`: Go to the SQL Warehouse you created and to connection details copy HTTP path.
* `token`: Follow [these instructions](https://docs.databricks.com/en/dev-tools/auth/pat.html#databricks-personal-access-tokens-for-workspace-users) to generate a personal access token.

**NB!** You never should store the token another place than in the .dbt/profiles.yml.

```yml
 transform:
  target: local_dev
  outputs:
    local_dev:
      type: databricks
      catalog: dev
      schema: firstname_lastname
      host: xyz.azuredatabricks.net # Need to be configured by you
      http_path: /SQL/YOUR/HTTP/PATH # Need to be configured configured by you
      token: dapiXXXXXXXXXXXXXXXXXXXXXXX # Need to be configured configured by you
      threads: 4
```


### 3. Activate virtual environment
* Enter the project folder: `cd projects/data-models`
* Activate the virtual environment: `poetry shell`
* Install dependencies if relevant: `poetry install`

Use docker instead (will rewrite this when implemented)
* install docker: https://docs.docker.com/desktop/install/windows-install/
* enter the directory projects/data-model
* run docker-compose run -it dev bash

### 4. Enter dbt project
Enter the dbt project: `cd transform`

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
When making changes to the project you must create a new branch. It's best practice to create branches for each type of change that you do. I.e., if you are creating a pipeline for loyalty this can be one branch, while you should create a new one when devloping a pipeline for another domain which are not related to the loyalty tables.

To create a new branch follow these steps:
* Pull last changes from main: `git pull`
* Create new branch: `git checkout -b "firstname/short-description-of-what-you-are-doing"`

An example of a branch could be `marie/add-loyalty-ledger`

If first time doing this as for help.

### 7. Start developing 🥳
Hurray! Now you can start developing. 

Make sure to follow the guidelines in the [Data Model Deveopment](#data-model-development) section.

### 8. Deploy changes from local environment
To assess that change made has the expected output you need to deploy your changes to Databricks.
First you have to run `dbt deps` to install packages, then you can deploy. To run only the model you have created or made changes to run `dbt build -s model_filename` in the terminal.

Run `dbt` in the terminal to see all the other available commands. Or read more about the commands [in dbts docs](https://docs.getdbt.com/reference/dbt-commands).

### 9. Add, commit and push changes
While doing changes during development you should add and commit you changes for each step taken towards the final result. Steps could for instance be: Adding a table to silver, adding documentation, fixing a bug etc.
The process is as follows:
* `git status`: list all the files that have been changed
* `git add [filename]`: add the files you want to commit
* `git commit -m`: "short description of what you did"
* `git push`: sends the changes to out github repo

If first time doing this ask for help.

### 10. Create pull request
When you are finished developing on the branch you can merge you changes into main. This is done through creating a pull request. You can do that in the [GitHub UI](https://github.com/cheffelo/sous-chef). If first time doing this ask for help.


# Pipeline Development

Need to be written...

# Data Model Development
This section contains information and best practices related to data modelling in dbt.

## Project structure

🥈 **Silver**: Contains models which clean and standardize data.  

🏄🏻 **Intermediate**: Intermediate steps to break down transformations towards the gold layer.

🥇 **Gold**: Contains models which combine or heavily transform data. 

🛠️ **Utilities**: General purpose models that we generate from macros or based on seeds that provide tools to help us do our modeling, rather than data to model itself. Could for instance be a date table. (Not in use yet).

## Modelling in Silver
The silver consist of models that are cleansed and standardized. The models are based on tables from the bronze layer in the database. No joins should be done in this layer unless there are very special cases which is explained further under [base models](#7).

### 1. Add model to _[sourcesystem]_source.yml
When adding new tables to the silver layer you first have to add the table to the `_sourcesystem_source.yml` file. To easily generate the code for this you can use the script below. Fill in the name of the tables in bronze that you want to add. This will output the code in the terminal for you to copypaste inton `_sourcesystem_source.yml`. Make sure to only copy the part below `tables` in the output.

```shell
dbt --quiet run-operation generate_source --args '{
  "schema_name": "bronze",
  "table_names": [
    "table_name_1",
    "table_name_2"
    ],
  "generate_columns": "true",
  "include_descriptions": "true",
  "name": cms
  }'
```

### 2. Add documentation
After adding the table to `_sourcesystem_source.yml` you should document the table and columns. This should be done by 1) adding the documentation inside `_<sourcesystem>__docs.md`, and 2) then refer to this in the `_sourcesystem_source.yml`. This process sucks a bit, and we should try to make it better.

To look at the documentation you have to spin it up locally on the computer. This is done by running `dbt docs generate` followed by `dbt docs serve` in the terminal.

It will look like this in the `<sourcesystem>__source.yml`:

```yml
  - name: this_is_a_column_with_source_naming
    data_type: some datatype
    description: "{{ doc('column__this_is_a_column_with_good_naming') }}"
```

And like this in `_<sourcesystem>__docs.md`:
```
{% docs column__this_is_a_column_with_good_naming %}

The primary key of the product type in the product layer.

{% enddocs %}
```

#### Naming conventions in docs
* source: sourcetype__sourcesystem, e.g., database__cms
* table: table__name_of_table, e.g, table__product_concepts
* column: column__name_of_column_in_model (do not use the source column name, but what it will be renamed to in the model)

#### Creating documentation is boring, but also an important step that should not be skipped, *because...*

![If not now, then when](https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExN2U1dW1haGhwMHg5Nzg5YWZpdHlpanA2Njc0MnZpbjg3cnFtd3VycyZlcD12MV9naWZzX3NlYXJjaCZjdD1n/ZEZIKjzPEjv2dKy1k4/giphy.gif)


### 3. Create model

#### 1. Find the correct folder
Each source has its own subdirectory in the silver folder where the models are placed. In addtion there is a base folder which is used for models that are joined or unioned with another main model. This should only be done if absolutly needed. For instance if having tables containing historic data.

#### 2. Create model file
The model file name should follow this naming convention: `sil_[sourcesystem]__[entity]s.sql`. (E.g, cms__companies, or cms__order_lines)

#### 3. Generate code
To get a head start you can generate a model script based by running the following code in your terminal, replacing `source_name` and `table_name` with the corresponding found in the `_[sourcesystem]__source.yml` file:

```shell
dbt run-operation generate_base_model --args '{
  "source_name": "cms", 
  "table_name": "cms_billing_agreement_order",
  "leading_commas": "true"
  }'
```

This will output code to the terminal that you can copypaste to the model-file.

#### 4. Organize code
* There should be CTEs for each bigger transformation step.
* The CTEs should be named by the main acitivty in them.
* The columns should be organized in categories by data types just as in the example.

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
- 💪 Consistency is key! Use the same field names across models where possible. For example, an id to the `customers` table should be named `customer_id` rather than `user_id`

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
  * 🎚️ Booleans should be prefixed with `is_` or `has_`, e.g., `is_active_customer` and `has_admin_access`
  * 💵 Price/revenue fields should be in decimal currency (for example, `19.99` for 19.99kr)
  * 🔑 For id columns that are used as primary keys in the source system should always be called `tablename_id`.
  
#### 7. Base models
In some situations one need to do joins or unioning too make a source table complete. This could be if there is a separate delete tables that holds information about which customers that are deleted (joining) or if there are history table holding historical records (unioning). In these cases the source tables should be placed in the the base folder and then create a final table by joining/unioning which is stored with the other final models.

### 3. Add model to models.yml
After creating the model you need to add it to the models.yml. The simplest way of doing this is to use the template below and copy all the columns from the `sourcesystem_source.yml`. Remember that you need to change the column names of columns that have been renamed. You also have to add an alias to give the table another name than the file name in the database.

```yml

 - name: sil_product_layer__product_types
    description: ""
    latest_version: 1
    config:
      alias: #sourcesystem_tablename

    columns:
    
    # INSERT CODE FROM SOURCE.YML
    # RENAME COLUMNS THAT HAVE BEEN RENAMED

    versions:
      - v: 1

```

### 5. Add tests
Relevant tests should be added. More info to be written...

### 6. Deploy
Deploy the models by running `dbt build -s model_filename` in the terminal

:::tip Read more
Read more about silver models (called staging in dbt) [here](https://docs.getdbt.com/best-practices/how-we-structure/2-staging).
:::

## Modelling in the Gold layer
The gold layer consist of models that are optimized for reporting. Before create the model in Gold you need to deploy the relevant models in Silver if you have not done this yet.

### 1. Intermediate transformation steps
Transformation logic should be as modular as possible. Hence, we have the intermediate folder which is meant for putting parts of the transformation logic.

#### 1. Find correct folders
Subdirectories are based on business groupings.

#### 2. Create model file
The model file name should describe the transformation output: `int_[entity]s_[verb]s.sql`. E.g, `int_billing_agreement_addon_subscriptions_pivoted`, or `int_billing_agreement_extract_first_order`)

#### 3. Organize code in CTEs
Each step of the transformation should be organized in CTEs with names that are describing the transformation being done. This makes it easier to debug the code and make changes to seperate parts of the code.

```sql
-- int_payments_pivoted_to_orders.sql

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

:::tip Read more
Read more about intermediate models [here](https://docs.getdbt.com/best-practices/how-we-structure/3-intermediate).
:::

### 3. Gold layer
The models in the gold layer can be put together by combining models from silver and intermediate. 

#### 1. Find correct folders
Subdirectories are based on business groupings.

#### 2. Create model file
The model file should start with fact or dim based on the type of table followed by the enitity in plain english: `[fact/dim]_[entity]s.sql`. E.g, `fact_orders`, `billing_agreements`. One should NOT create models with the same concept for several teams. I.e., there should not be a table for finance_orders and marketing_orders.

#### 3. Best practice and naming
* Name of the model should be the same as the file name
* All dimensional tables should have a primary key called `pk_<dim>_<tablename>`. This should be created by creating an hash by concatenating the columns needed for it to be unique by using the MD5() function.

#### 4. Add model to model.yml
The model need to be added to the model.yml in the subdirectory of the business group in the gold folder. In this case you can copy from the `model.yml` in silver and do the needed adjustments. One need to have an alias eventhough its the same as the filename since when working with several version one will add _v1 etc to the files (need to add in more about versioning in the docs).

#### 5. Add documentation
Then we need to document some more. Need to figure out how to organize this.

#### 6. Add tests
Relevant tests should be added. More info to be written...

#### 7. Deploy model
Deploy the models by running `dbt deps` to install packages and `dbt build -s model_filename` to deploy the specific model to databricks. 

:::tip Read more
Read more about gold models (called marts in dbt) [here](https://docs.getdbt.com/best-practices/how-we-structure/4-marts).
:::

### Why dbt recommend using CTEs

For more information about why we use so many CTEs, read [this glossary entry](https://docs.getdbt.com/terms/cte).

- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as verbose as needed to convey what they do.
- CTEs with confusing or noteable logic should be commented with SQL comments as you would with any complex functions and should be located above the CTE.
- CTEs duplicated across models should be pulled out and created as their own models.


- Only models in `silver` should select from [sources](https://docs.getdbt.com/docs/building-a-dbt-project/using-sources).
- Models not in the `silver` folder should select from [refs](https://docs.getdbt.com/reference/dbt-jinja-functions/ref).

## Target folder
The code you create will be translated to the right syntax fro Databricks. The compiled code can be found in the target folder. This can be useful to look at if you experience some troubles with your code. 

You can run `dbt compile` in the terminal to just compile the code with out deploying to Databricks to look at how it will turn out. 

The target folder will keep scripts from models you have deleted. To clean this up you can simply just delete the folder as it will be regenerated next time you run `dbt compile` or `dbt build`, or you can run `dbt clean` which will also delete the folder. 

# DBT packages and macros
...

# Style guides
Just copypasted this. Need to look more into this.  

## SQL
### Basics
- ☁️ Use [SQLFluff](https://sqlfluff.com/) to maintain these style rules automatically.
  - Customize `.sqlfluff` configuration files to your needs.
  - Refer to our [SQLFluff config file](https://github.com/dbt-labs/jaffle-shop-template/blob/main/.sqlfluff) for the rules we use in our own projects. 

  - Exclude files and directories by using a standard `.sqlfluffignore` file. Learn more about the syntax in the [.sqlfluffignore syntax docs](https://docs.sqlfluff.com/en/stable/configuration.html#id2).
- 👻 Use Jinja comments (`{# #}`) for comments that should not be included in the compiled SQL.
- ⏭️ Use trailing commas.
- 4️⃣ Indents should be four spaces.
- 📏 Lines of SQL should be no longer than 80 characters.
- ⬇️ Field names, keywords, and function names should all be lowercase.
- 🫧 The `as` keyword should be used explicitly when aliasing a field or table.

:::info
☁️ dbt Cloud users can use the built-in [SQLFluff Cloud IDE integration](https://docs.getdbt.com/docs/cloud/dbt-cloud-ide/lint-format) to automatically lint and format their SQL. The default style sheet is based on dbt Labs style as outlined in this guide, but you can customize this to fit your needs. No need to setup any external tools, just hit `Lint`! Also, the more opinionated [sqlfmt](http://sqlfmt.com/) formatter is also available if you prefer that style.
:::

### Fields, aggregations, and grouping

- 🔙 Fields should be stated before aggregates and window functions.
- 🤏🏻 Aggregations should be executed as early as possible (on the smallest data set possible) before joining to another table to improve performance.
- 🔢 Ordering and grouping by a number (eg. group by 1, 2) is preferred over listing the column names (see [this classic rant](https://www.getdbt.com/blog/write-better-sql-a-defense-of-group-by-1) for why). Note that if you are grouping by more than a few columns, it may be worth revisiting your model design.

### Joins

- 👭🏻 Prefer `union all` to `union` unless you explicitly want to remove duplicates.
- 👭🏻 If joining two or more tables, _always_ prefix your column names with the table name. If only selecting from one table, prefixes are not needed.
- 👭🏻 Be explicit about your join type (i.e. write `inner join` instead of `join`).
- 🥸 Avoid table aliases in join conditions (especially initialisms) — it's harder to understand what the table called "c" is as compared to "customers".
- ➡️ Always move left to right to make joins easy to reason about - `right joins` often indicate that you should change which table you select `from` and which one you `join` to.
- 👈 Left join should be the preferred join unless another join is required.
- 🔑 When performing a left join always set the left table key on the left side in ON clause.

### 'Import' CTEs

- 🔝 All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file.
- 📦 'Import' CTEs should be named after the table they are referencing.
- 🤏🏻 Limit the data scanned by CTEs as much as possible. Where possible, only select the columns you're actually using and use `where` clauses to filter out unneeded data.
- For example:

```sql
with

orders as (

    select
        order_id,
        customer_id,
        order_total,
        order_date

    from {{ ref('orders') }}

    where order_date >= '2020-01-01'

)
```

### 'Functional' CTEs

- ☝🏻 Where performance permits, CTEs should perform a single, logical unit of work.
- 📖 CTE names should be as verbose as needed to convey what they do e.g. `events_joined_to_users` instead of `user_events` (this could be a good model name, but does not describe a specific function or transformation).
- 🌉 CTEs that are duplicated across models should be pulled out into their own intermediate models. Look out for chunks of repeated logic that should be refactored into their own model.
- 🔚 The last line of a model should be a `select *` from your final output CTE. This makes it easy to materialize and audit the output from different steps in the model as you're developing it. You just change the CTE referenced in the `select` statement to see the output from that step.

### Model configuration

- 📝 Model-specific attributes (like sort/dist keys) should be specified in the model.
- 📂 If a particular configuration applies to all models in a directory, it should be specified in the `dbt_project.yml` file.
- 👓 In-model configurations should be specified like this for maximum readability:

```sql
{{
    config(
      materialized = 'table',
      sort = 'id',
      dist = 'id'
    )
}}
```

## Python

### Python tooling

- 🐍 Python has a more mature and robust ecosystem for formatting and linting (helped by the fact that it doesn't have a million distinct dialects). We recommend using those tools to format and lint your code in the style you prefer.

- 🛠️ Our current recommendations are

  - [black](https://pypi.org/project/black/) formatter
  - [ruff](https://pypi.org/project/ruff/) linter

  :::info
  ☁️ dbt Cloud comes with the [black formatter built-in](https://docs.getdbt.com/docs/cloud/dbt-cloud-ide/lint-format) to automatically lint and format their SQL. You don't need to download or configure anything, just click `Format` in a Python model and you're good to go!
  :::

### Example Python

```python
import pandas as pd


def model(dbt, session):
    # set length of time considered a churn
    pd.Timedelta(days=2)

    dbt.config(enabled=False, materialized="table", packages=["pandas==1.5.2"])

    orders_relation = dbt.ref("stg_orders")

    # converting a DuckDB Python Relation into a pandas DataFrame
    orders_df = orders_relation.df()

    orders_df.sort_values(by="ordered_at", inplace=True)
    orders_df["previous_order_at"] = orders_df.groupby("customer_id")[
        "ordered_at"
    ].shift(1)
    orders_df["next_order_at"] = orders_df.groupby("customer_id")["ordered_at"].shift(
        -1
    )
    return orders_df
```

## Jinja

- 🫧 When using Jinja delimiters, use spaces on the inside of your delimiter, like `{{ this }}` instead of `{{this}}`
- 🆕 Use newlines to visually indicate logical blocks of Jinja.
- 4️⃣ Indent 4 spaces into a Jinja block to indicate visually that the code inside is wrapped by that block.
- ❌ Don't worry (too much) about Jinja whitespace control, focus on your project code being readable. The time you save by not worrying about whitespace control will far outweigh the time you spend in your compiled code where it might not be perfect.

### Examples of Jinja style

```jinja
{% macro make_cool(uncool_id) %}

    do_cool_thing({{ uncool_id }})

{% endmacro %}
```

```sql
select
    entity_id,
    entity_type,
    {% if this %}

        {{ that }},

    {% else %}

        {{ the_other_thing }},

    {% endif %}
    {{ make_cool('uncool_id') }} as cool_id
```

## YAML

- 2️⃣ Indents should be two spaces
- ➡️ List items should be indented
- 🆕 Use a new line to separate list items that are dictionaries where appropriate
- 📏 Lines of YAML should be no longer than 80 characters.
- 🛠️ Use the [dbt JSON schema](https://github.com/dbt-labs/dbt-jsonschema) with any compatible IDE and a YAML formatter (we recommend [Prettier](https://prettier.io/)) to validate your YAML files and format them automatically.

### Example YAML

```yaml
version: 2

models:
  - name: events
    columns:
      - name: event_id
        description: This is a unique identifier for the event
        tests:
          - unique
          - not_null

      - name: event_time
        description: "When the event occurred in UTC (eg. 2018-01-01 12:00:00)"
        tests:
          - not_null

      - name: user_id
        description: The ID of the user who recorded the event
        tests:
          - not_null
          - relationships:
              to: ref('users')
              field: id
```


