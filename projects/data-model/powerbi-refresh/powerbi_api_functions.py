import requests
import json
from typing import Optional

def get_headers_with_token(access_token: str) -> dict:
    
    headers_with_token = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
    
    return headers_with_token


def get_access_token(
    client_id: str,
    client_secret: str,
    tenant_id: str,
) -> str:
    
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
        
        return access_token

    else:
        raise Exception(f"Failed to retrieve access token. Status code: {response.status_code}. Response: {response.text}")
        

def get_workspace_id(
    pbi_workspace_name: str,
    access_token: str,
) -> str:
    '''
    Fetches the workspace_id of the specified workspace name from PowerBI.

    Args:
        pbi_workspace_name (str): Name of the PowerBI workspace.
        access_token (str): Access token generated for the client.

    Returns:
        str: The workspace ID
    '''

    url = "https://api.powerbi.com/v1.0/myorg/groups/"
    headers_with_token = get_headers_with_token(access_token)
    
    response = requests.get(url, headers=headers_with_token)

    if response.status_code == 200:
        workspaces = response.json()['value']
        
        workspace_name = pbi_workspace_name
        workspace_id = next((workspace['id'] for workspace in workspaces if workspace['name'] == workspace_name), None)
        
        if workspace_id:
            print(f"Workspace ID for '{workspace_name}': {workspace_id}")
            return workspace_id
        else:
            raise Exception(f"No workspace found with the name '{workspace_name}'")
    else:
        raise Exception(f"Failed to retrieve workspaces. Status code: {response.status_code}. Response: {response.text}")


def get_dataset_id(
    semantic_model_name: str,
    workspace_id: str,
    access_token: str,
) -> str:
    '''
    Fetches the dataset ID of the specified Semantic Model from PowerBI.

    Args:
        semantic_model_name (str): Name of the Semantic Model.
        workspace_id (str): ID of the workspace.
        access_token (str): Access token generated for the client.

    Returns:
        str: The dataset ID.
    '''
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    headers_with_token = get_headers_with_token(access_token)
    response = requests.get(url, headers=headers_with_token)

    if response.status_code == 200:
        datasets = response.json()['value']
        
        dataset_name = semantic_model_name
        dataset_id = next((dataset['id'] for dataset in datasets if dataset['name'] == dataset_name), None)
        
        if dataset_id:
            print(f"Semantic model ID for '{dataset_name}': {dataset_id}")
            return dataset_id
        else:
            raise Exception(f"No semantic model found with the name '{dataset_name}'")
    else:
        raise Exception(f"Failed to retrieve workspaces. Status code: {response.status_code}. Response: {response.text}")


def get_powerbi_tables(
    workspace_id: str, 
    dataset_id: str, 
    access_token: str
) -> list[str]:
    '''
    Fetches table names from PowerBI in the specified Semantic Model.

    Args:
        workspace_id (str): ID of the PowerBI workspace.
        dataset_id (str): ID of the Semantic Model.
        access_token (str): Access token generated for the client.

    Returns:
        list(str): A list of table names in the PowerBI workspace.
    '''

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
    headers = get_headers_with_token(access_token)

    # DAX query to get table information
    payload = {
        "queries": [
            {
                "query": "SELECT DISTINCT TABLE_NAME FROM $SYSTEM.DBSCHEMA_TABLES WHERE TABLE_TYPE = 'TABLE'"
            }
        ]
    }

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        result = response.json()
    
        powerbi_tables = [item['TABLE_NAME'][1:] for item in result['results'][0]['tables'][0]['rows']]
    
        return powerbi_tables
    
    else:
        raise Exception(f'Failed to fetch tables from PowerBI. {response}')


def refresh_tables(
    workspace_id: str,
    dataset_id: str,
    access_token: str,
    tables: Optional[str] = '',
) -> int:
    '''
   Refreshes selected tables in the PowerBI workspace if provided, else executes a full refresh. 

    Args:
        workspace_id (str): ID of the PowerBI workspace.
        dataset_id (str): ID of the Semantic Model.
        access_token (str): Access token generated for the client.
        tables (str, optional): "A string of, comma separated tables, written, like, this"

    Returns:
        str: Result of refresh
    '''
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    headers_with_token = get_headers_with_token(access_token)

    # Refresh full semantic model if no specified tables
    if not tables:
        response = requests.post(url, headers=headers_with_token)

        if response.status_code == 202:
            return 'Full table refresh initiated'
        else:
            raise Exception(f"Failed to refresh PowerBI. Status code: {response.status_code}. Response: {response.text}")       

    else:
        table_list = [item.strip() for item in tables.split(',')]
        
        # Fetch all table names in the dataset in the PowerBI workspace
        powerbi_tables = get_powerbi_tables(workspace_id, dataset_id, access_token)

        # Refresh selected tables
        tables_to_refresh = [table for table in table_list]

        for table in tables_to_refresh:
            if table not in powerbi_tables:
                raise Exception(f"Table '{table}' not found in PowerBI workspace.")


        # Build refresh request for specific tables
        refresh_body = {
            "type": "full",
            "commitMode": "transactional", # roll-back refresh if any table fails to refresh
            "maxParallelism": 2, # refresh two tables at a time
            "retryCount": 2,
            "objects": [
                {
                    "table": table_name
                }
                for table_name in tables_to_refresh
            ]
        }
                
        refresh_response = requests.post(url, headers=headers_with_token, json=refresh_body)
                
        if refresh_response.status_code == 202:
            return f"Refresh initiated successfully for selected tables: {tables_to_refresh}."
        else:
            raise Exception(f"Failed to refresh tables. Status code: {refresh_response.status_code}. Response: {refresh_response.text}")