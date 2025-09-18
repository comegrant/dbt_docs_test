# Databricks notebook source
import sys
sys.path.append('../../reusable')

import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
orders_source_table = "billing_agreement_order"
orders_year_source_column = "year"
orders_week_source_column = "week"
orders_join_source_column = "id"

orders_silver_table = f"silver.cms__{orders_source_table}s"
orders_silver_date_column = "menu_week_monday_date"

order_lines_source_table = "billing_agreement_order_line"
order_lines_join_source_column = "agreement_order_id"

# COMMAND ----------

# Number of weeks back in time to extract data from based on parameter or default value
dbutils.widgets.text("incremental_load_weeks", "8")

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
    query_orders_source = (
        f"""(
            SELECT * FROM {orders_source_table} 
            WHERE {orders_week_source_column} = {week}
            AND {orders_year_source_column} = {year}
        )"""
    )
    
    # Load the data for this week
    load_coredb_query(dbutils, database, orders_source_table, query_orders_source, append_flag)
    print(f"Order data for year {year}, week {week} is loaded")
    
    # Query for order lines that belong to the orders for this specific week
    query_order_lines_source = (
        f"""(
            SELECT order_lines.* 
            FROM {orders_source_table} as orders
            INNER JOIN {order_lines_source_table} as order_lines 
            ON orders.{orders_join_source_column} = order_lines.{order_lines_join_source_column} 
            WHERE orders.{orders_week_source_column} = {week}
            AND orders.{orders_year_source_column} = {year}
        )"""
    )
    
    print(f"Query 2 for year {year}, week {week}:")
    print(query_order_lines_source)
    
    # Load the order lines data for this week
    load_coredb_query(dbutils, database, order_lines_source_table, query_order_lines_source, append_flag)

    print(f"Order Line data for year {year}, week {week} is loaded")

    # Set append_flag to True after the first iteration
    append_flag = True

    print("=" * 50)

print("All weeks processed successfully!")
