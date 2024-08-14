# Databricks notebook source
import sys


from calendars import *

# COMMAND ----------


dates = get_calendar_dataframe("2018-01-01", "2030-12-31")

# COMMAND ----------

dates.head()

# COMMAND ----------

dates['year'] = dates['year'].astype('int32')
dates['week'] = dates['week'].astype('int32')

# COMMAND ----------

dates = dates.rename(columns={'datekey': 'pk_dim_date'})

# COMMAND ----------

dates = spark.createDataFrame(dates)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.silver_calendar

# COMMAND ----------

dates.write.mode("overwrite").saveAsTable("silver.silver_calendar")
