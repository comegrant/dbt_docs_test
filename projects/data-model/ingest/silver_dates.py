# Databricks notebook source
from calendars import *

# COMMAND ----------

dates = get_calendar_dataframe("2015-01-01", "2030-12-31")

# COMMAND ----------

dates = spark.createDataFrame(dates)

# COMMAND ----------

dates = dates \
            .withColumn("date", dates["date"].cast("date"))\
            .withColumnRenamed("datekey", "pk_dim_dates")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.databricks__dates

# COMMAND ----------

dates.write.mode("overwrite").saveAsTable("silver.databricks__dates")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE silver.databricks__dates IS 'This is a table generated in the Databricks data platform. The table will be used to create gold.dim_dates that contains the date information in the data model';
