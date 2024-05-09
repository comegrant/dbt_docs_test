# Databricks notebook source
database_name = "CMS"
tables = ["company", "billing_agreement", "billing_agreement_status", "billing_agreement_order", "billing_agreement_order_line", "address_live", "address_history"]
host = "brandhub-fog.database.windows.net"
username = dbutils.secrets.get('auth_common','coreDbUsername')
password = dbutils.secrets.get('auth_common','coreDbPassword')

# COMMAND ----------

for table in tables:
  remote_table = (spark.read
    .format("sqlserver")
    .option("host", host)
    .option("port", "1433")
    .option("user", username)
    .option("password", password)
    .option("database", database_name)
    .option("dbtable", table)
    .load()
  )

  print(f"dev.temppocbronze.{database_name}_{table}")

  remote_table.write.mode("overwrite").saveAsTable(f"dev.temppocbronze.{database_name}_{table}")

# COMMAND ----------

database_name = "PRODUCT_LAYER"
tables = [
    'product',
    'product_type',
    'product_variation',
    'product_variation_company',
    'product_type_concept',
    'product_concept',
    'product_variation_attribute_value',
    'product_variation_attribute_template'
]
host = "brandhub-fog.database.windows.net"
username = dbutils.secrets.get('auth_common','coreDbUsername')
password = dbutils.secrets.get('auth_common','coreDbPassword')

# COMMAND ----------

for table in tables:
  remote_table = (spark.read
    .format("sqlserver")
    .option("host", host)
    .option("port", "1433")
    .option("user", username)
    .option("password", password)
    .option("database", database_name)
    .option("dbtable", table)
    .load()
  )

  print(f"dev.temppocbronze.{database_name}_{table}")

  remote_table.write.mode("overwrite").saveAsTable(f"dev.temppocbronze.{database_name}_{table}")
