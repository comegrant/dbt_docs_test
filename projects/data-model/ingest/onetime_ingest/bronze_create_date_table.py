# Databricks notebook source
from calendars import *

# COMMAND ----------

dates = get_calendar_dataframe("2005-01-01", "2030-12-31")

# COMMAND ----------

dates = spark.createDataFrame(dates)

# COMMAND ----------

dates = dates \
            .withColumn("date", dates["date"].cast("date"))\
            .withColumnRenamed("datekey", "pk_dim_dates")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze.data_platform__dates

# COMMAND ----------

dates.write.mode("overwrite").saveAsTable("bronze.data_platform__dates")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE bronze.data_platform__dates IS 'This is a table generated in the Databricks data platform. The table will be used to create gold.dim_dates that contains the date information in the data model';
