# Databricks notebook source
import requests

# COMMAND ----------

#Authenticate to Power BI
client_id = dbutils.secrets.get(scope="auth_common", key="azure-sp-powerbi-clientId")
client_secret = dbutils.secrets.get(scope="auth_common", key="azure-sp-powerbi-clientSecret")
tenant_id = dbutils.secrets.get(scope="auth_common", key="azure-tenant-cheffelo-tenantId")

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

body = {
    'grant_type': 'client_credentials',
    'scope': 'https://analysis.windows.net/powerbi/api/.default',
    'client_id': client_id,
    'client_secret': client_secret
}

headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
}

response = requests.post(token_url, headers=headers, data=body)

if response.status_code == 200:
    token_response = response.json()
    access_token = token_response['access_token']
    print("Access token retrieved successfully!")
    
    headers_with_token = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

else:
    print(f"Failed to retrieve access token. Status code: {response.status_code}")
    print(f"Response: {response.text}")

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
url = "https://api.powerbi.com/v1.0/myorg/groups/"
response = requests.get(url, headers=headers_with_token)

if response.status_code == 200:
    workspaces = response.json()['value']
    
    workspace_name = pbi_workspace_name
    workspace_id = next((workspace['id'] for workspace in workspaces if workspace['name'] == workspace_name), None)
    
    if workspace_id:
        print(f"Workspace ID for '{workspace_name}': {workspace_id}")
    else:
        print(f"No workspace found with the name '{workspace_name}'")
else:
    print(f"Failed to retrieve workspaces. Status code: {response.status_code}")
    print(f"Response: {response.text}")

# COMMAND ----------

#Get Power Bi semantic model id 
url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
response = requests.get(url, headers=headers_with_token)

if response.status_code == 200:
    datasets = response.json()['value']
    
    dataset_name = semantic_model_name
    dataset_id = next((dataset['id'] for dataset in datasets if dataset['name'] == dataset_name), None)
    
    if dataset_id:
        print(f"Semantic model ID for '{dataset_name}': {dataset_id}")
    else:
        print(f"No semantic model found with the name '{dataset_name}'")
else:
    print(f"Failed to retrieve workspaces. Status code: {response.status_code}")
    print(f"Response: {response.text}")

# COMMAND ----------

#Refresh semantic model
url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"

response = requests.post(url, headers=headers_with_token)

if response.status_code == 202:
    print("Dataset refresh initiated successfully!")
else:
    print(f"Failed to initiate refresh. Status code: {response.status_code}")
    print(f"Response: {response.text}")