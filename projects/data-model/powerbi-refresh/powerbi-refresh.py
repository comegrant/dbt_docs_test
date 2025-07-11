# Databricks notebook source
import requests
import sys
import powerbi_api_functions as pb

# COMMAND ----------

# Set selected tables to refresh if passed as a parameter, else refresh all tables
tables = dbutils.widgets.get("tables")

# COMMAND ----------

#Authenticate to Power BI
client_id = dbutils.secrets.get(scope="auth_common", key="azure-sp-powerbi-clientId")
client_secret = dbutils.secrets.get(scope="auth_common", key="azure-sp-powerbi-clientSecret")
tenant_id = dbutils.secrets.get(scope="auth_common", key="azure-tenant-cheffelo-tenantId")

access_token = pb.get_access_token(client_id, client_secret, tenant_id)

headers_with_token = pb.get_headers_with_token(access_token)

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
pbi_workspace_name = ''
if catalog == 'dev':
    pbi_workspace_name = 'Cheffelo [dev]'
elif catalog == 'test':
    pbi_workspace_name = 'Cheffelo [test]'
elif catalog == 'prod':
    pbi_workspace_name = 'Cheffelo'
else:
    print('Power BI workspace name is not set correctly')

# COMMAND ----------

semantic_model_name = 'Main Data Model'

# COMMAND ----------

#Get Power Bi workspace id 
workspace_id = pb.get_workspace_id(pbi_workspace_name, access_token)


# COMMAND ----------

#Get Power Bi semantic model id 
dataset_id = pb.get_dataset_id(semantic_model_name, workspace_id, access_token)

# COMMAND ----------

# Trigger full refresh or selected models
result = pb.refresh_tables(workspace_id, dataset_id, access_token, tables)
print(result)
    

