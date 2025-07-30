# Databricks notebook source
%pip install aiohttp>=3.12.14
# COMMAND ----------

from customerio_utilities import CustomerIOSyncer
import logging
import sys

logging.basicConfig(stream=sys.stdout, force=True)

# COMMAND ----------

dbutils.widgets.text("brand_shortname", "test", "Brand shortname: AMK, GL, LMK, RT, or test")
dbutils.widgets.text("company_id", "")
dbutils.widgets.text("workspace_env", "", "dev, test, or prod")

brand_shortname = dbutils.widgets.get("brand_shortname")
company_id = dbutils.widgets.get("company_id")
workspace_env = dbutils.widgets.get("workspace_env")

traits_table = "diamond.billing_agreement_traits"
sync_status_table = "bronze.data_platform__billing_agreement_customerio_syncs"

cio_syncer = CustomerIOSyncer(
    dbutils, 
    brand_shortname=brand_shortname, 
    company_id=company_id,
    traits_table=traits_table,
    sync_status_table=sync_status_table,
    chunk_size=5000,
    max_concurrent=50
)

# COMMAND ----------

if workspace_env == 'prod':
    results = await cio_syncer.sync_all_customers_with_updates()
else:
    df = cio_syncer.get_customers_with_updates()
    results = f"Found {len(df)} agreements to sync on {brand_shortname}. This sync will not occur as it is in {workspace_env}"
    
print(results)