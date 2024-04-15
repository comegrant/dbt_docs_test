# Databricks notebook source
database_name = "CMS"
tables = ["company","country"]
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

  print(f"dev.bronze.{{database_name}_table}")

  remote_table.write.mode("overwrite").saveAsTable(f"dev.bronze.{database_name}_{table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dev.bronze.cms_company ALTER COLUMN id SET NOT NULL;
# MAGIC ALTER TABLE dev.bronze.cms_company ADD CONSTRAINT company_pk PRIMARY KEY(id);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dev.bronze.cms_country ALTER COLUMN id SET NOT NULL;
# MAGIC ALTER TABLE dev.bronze.cms_country ADD CONSTRAINT country_pk PRIMARY KEY (id);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dev.bronze.cms_company ADD CONSTRAINT company_fk FOREIGN KEY (country_id) REFERENCES dev.bronze.cms_country(id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.bronze.cms_company
