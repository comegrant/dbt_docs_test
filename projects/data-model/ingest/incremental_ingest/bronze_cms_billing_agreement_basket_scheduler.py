# Databricks notebook source
import sys
sys.path.append('../../reusable')

import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
source_table = "billing_agreement_basket_scheduler"
source_year_column = "year"
source_week_column = "week"

silver_table = f"silver.cms__{source_table}"
silver_year_column = "menu_year"
silver_week_column = "menu_week"

# COMMAND ----------

# Number of weeks back in time to extract data from based on parameter or default value
dbutils.widgets.text("incremental_load_weeks", "8")

weeks_back = int(dbutils.widgets.get("incremental_load_weeks"))

# COMMAND ----------

query_silver = (
    f"""(
        SELECT
            MAX_BY(
                {silver_year_column}
                , {silver_year_column}*100+{silver_week_column}
            ) as max_year
            , MAX_BY(
                {silver_week_column}
                , {silver_year_column}*100+{silver_week_column}
            ) as max_week
        FROM {silver_table}
        WHERE menu_week_monday_date < current_date()
    )"""
)

# COMMAND ----------

print(query_silver)

# COMMAND ----------

try: 
    spark.table(f"{silver_table}")
        
    # Find max week and year of corresponding silver table
    from_year_week_df = spark.sql(query_silver)
    max_year = from_year_week_df.collect()[0][f'max_year']
    max_week = from_year_week_df.collect()[0][f'max_week']

    # Handle corner cases where source systems add week 53 to their system even when it does not exist in the calendar
    if max_week == 53:
        max_week = 52

    # Get Monday date of max week and year
    max_date = datetime.date.fromisocalendar(max_year, max_week, 1)

    # Get the ISO year and week number to extract data from
    from_date = max_date - datetime.timedelta(weeks=weeks_back)
    from_year, from_week, _ = from_date.isocalendar()

except Exception as e:
    from_year = 2016 #first year of data
    from_week = 1

# COMMAND ----------

query = (
    f"""(
        SELECT * FROM {source_table} 
        WHERE {source_week_column} >= {from_week}
        AND {source_year_column} >= {from_year}
    )"""
)

# COMMAND ----------

print(query)

# COMMAND ----------

load_coredb_query(dbutils, database, source_table, query)
