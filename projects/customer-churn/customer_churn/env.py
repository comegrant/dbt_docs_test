import os


# Adding info on which schema to use for events for various companies
class CompanySchema:
    LMK = "javascript_lmk"
    AMK = "javascript_adams"
    GL = "js"
    RN = "javascript_retnment"


LABEL_TEXT_ACTIVE = "active"
LABEL_TEXT_DELETED = "deleted"
LABEL_TEXT_CHURNED = "churned"


AZURE_WORKSPACE_RESOURCE_ID = os.getenv("AZURE_WORKSPACE_RESOURCE_ID")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
DATABRICKS_HOST = os.getenv(
    "DATABRICKS_HOST",
    default="https://adb-3551439538502554.14.azuredatabricks.net",
)
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", default="databricks")
