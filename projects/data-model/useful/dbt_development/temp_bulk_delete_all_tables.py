# Databricks notebook source
developer = dbutils.widgets.get("firstname_lastname")

allowed_prefixes = {"synnelouise_andersen", "daniel_geback", "agathe_raaum", "grant_levang", "lina_sjolin", "stephen_allwright", "sylvia_liu"}

if not developer or developer not in allowed_prefixes:
    raise ValueError("The 'firstname_lastname' parameter does not contain an accepted value.")

# COMMAND ----------

schemas = spark.sql("SHOW SCHEMAS")

# COMMAND ----------

allowed_suffixes = {"silver", "intermediate", "gold", "mlgold"}

dbt_schemas = [row["databaseName"] for row in schemas.collect() if row["databaseName"].startswith(f"{developer}")]

dbt_schemas = [
    db_name for db_name in dbt_schemas 
    if db_name.split("_")[-1] in allowed_suffixes
]

# COMMAND ----------

print(dbt_schemas)

# COMMAND ----------

for dbt_schema in dbt_schemas:
    # Get all tables in the schema
    tables = spark.sql(f"SHOW TABLES IN `{dbt_schema}`")

    # Drop each table in the schema
    for table in tables.collect():
        spark.sql(f"DROP TABLE `{dbt_schema}`.`{table[1]}`")

    print(f"Dropped all tables in schema: {dbt_schema}")