# Databricks notebook source
import sys
sys.path.append('../../reusable')

import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------


# Orders in source
database = "operations"
orders_source_table = "orders_history"
orders_source_week_column = "week_nr"
orders_source_year_column = "year_nr"
orders_source_join_column = "order_id"

# Orders in silver
orders_silver_table = f"silver.base_operations__{orders_source_table}"
orders_silver_date_column = "menu_week_monday_date"

# Collis in source
collis_source_table = "order_collis_history"
collis_source_join_column = "order_id"


# COMMAND ----------

# Number of weeks back in time to extract data from based on parameter or default value
dbutils.widgets.text("incremental_load_weeks", "2")

weeks_back = int(dbutils.widgets.get("incremental_load_weeks"))-1

# COMMAND ----------

query_orders_silver = (
    f"""(
        SELECT
            MAX(
                {orders_silver_date_column}
            ) as max_date
        FROM {orders_silver_table}
        WHERE {orders_silver_date_column} < current_date()
    )"""
)

# COMMAND ----------

print(query_orders_silver)

# COMMAND ----------

try: 
    spark.table(f"{orders_silver_table}")
        
    # Find max week and year of corresponding silver table
    from_date_df = spark.sql(query_orders_silver)
    end_date = from_date_df.collect()[0][f'max_date']

    # Get the ISO year and week number to extract data from
    start_date = end_date - datetime.timedelta(weeks=weeks_back)

except Exception as e:
    print(f"Error: {e}")
    raise SystemExit("Ingest not started due to problems with start_date and end_date.")

# COMMAND ----------

print(f"start_date: {start_date}")
print(f"end_date: {end_date}")

# COMMAND ----------

weeks_to_process = []
current_date = start_date

while current_date <= end_date:
    year, week, _ = current_date.isocalendar()
    weeks_to_process.append((year, week))
    current_date += datetime.timedelta(weeks=1)
    print(f"Added week: {year}, {week}")

print(f"Processing {len(weeks_to_process)} weeks from {start_date} to {end_date}")

# COMMAND ----------

# Process each week individually

append_flag = False  # Start with False for the first iteration

for year, week in weeks_to_process:
    print(f"Processing year {year}, week {week}")
    
    # Query for billing agreement orders for this specific week
    # Do explicit select to avoid extracting GDPR related columns
    query_orders_source = (
        f"""(
            SELECT
                ID
                ,ORDER_ID
                ,AGREEMENT_ID
                ,ORDER_STATUS_ID
                ,CREATED_BY
                ,CREATED_DATE
                ,MODIFIED_BY
                ,MODIFIED_DATE
                ,ROUTE_ID
                ,AGREEMENT_POSTALCODE
                ,WEEKDAY
                ,AGREEMENT_TIMEBLOCK
                ,[FROM]
                ,[TO]
                ,DELIVERY_DATE
                ,CITY
                ,WEEK_NR
                ,YEAR_NR
                ,INI_COST
                ,COMPANY_ID
                ,ZONE_ID
                ,TRANSPORT_COMPANY_ID
                ,COST1DROP
                ,COST2DROP
                ,TRANSPORT_NAME
                ,ORDER_ID_REFERENCE
                ,ORDER_TYPE
                ,POD_PLAN_COMPANY_ID
                ,LAST_MILE_HUB_DISTRIBUTION_CENTER_ID
                ,CONNECTED_HUB
                ,BAO_ID
                ,WHEN_TO_DETAIL_PLAN
                ,customer_fee
                ,country_id
                ,external_logistics_id
                ,logistics_drop_price
                ,logistics_other_cost
                ,logistic_system_id
                ,external_order_id
            FROM {orders_source_table} 
            WHERE {orders_source_week_column} = {week}
            AND {orders_source_year_column} = {year}
        )"""
    )
    
    # Load the data for this week
    load_coredb_query(dbutils, database, orders_source_table, query_orders_source, append_flag)
    print(f"Order data for year {year}, week {week} is loaded")

    # Query for order lines that belong to the orders for this specific week
    query_collis_source = (
        f"""(
            SELECT collis.* 
            FROM {orders_source_table} as orders
            INNER JOIN {collis_source_table} as collis
            ON orders.{orders_source_join_column} = collis.{collis_source_join_column} 
            WHERE orders.{orders_source_week_column} = {week}
            AND orders.{orders_source_year_column} = {year}
        )"""
    )
    
    print(f"Query 2 for year {year}, week {week}:")
    print(query_collis_source)
    
    # Load the order lines data for this week
    load_coredb_query(dbutils, database, collis_source_table, query_collis_source, append_flag)
    print(f"Colli data for year {year}, week {week} is loaded")

    # Set append_flag to True after the first iteration
    append_flag = True

    print("=" * 50)

print("All weeks processed successfully!")
