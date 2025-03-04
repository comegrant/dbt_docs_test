from catalog_connector.config import DatabricksConnectionConfig
from catalog_connector.value import EnvironmentValue

# Fine to create here, as it is not connecting to anything, only creating the config
session_or_serverless = DatabricksConnectionConfig.session_or_serverless()

__all__ = [
    "DatabricksConnectionConfig",
    "EnvironmentValue",
    "session_or_serverless",
]
