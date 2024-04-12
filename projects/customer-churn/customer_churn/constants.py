import os


# Adding info on which schema to use for events for various companies
class CompanySchema:
    LMK = "javascript_lmk"
    AMK = "javascript_adams"
    GL = "js"
    RN = "javascript_retnment"


# Company_ids to be used for snapshot date
class CompanyID:
    LMK = "6A2D0B60-84D6-4830-9945-58D518D27AC2"
    AMK = "8A613C15-35E4-471F-91CC-972F933331D7"
    GL = "09ECD4F0-AE58-4539-8E8F-9275B1859A19"
    RN = "5E65A955-7B1A-446C-B24F-CFE576BF52D7"


LABEL_TEXT_ACTIVE = "active"
LABEL_TEXT_DELETED = "deleted"
LABEL_TEXT_CHURNED = "churned"


AZURE_WORKSPACE_RESOURCE_ID = os.getenv("AZURE_WORKSPACE_RESOURCE_ID")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", default="https://adb-3551439538502554.14.azuredatabricks.net")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", default="databricks")
