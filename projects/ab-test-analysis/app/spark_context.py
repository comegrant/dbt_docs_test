from databricks.connect import DatabricksSession


def get_serverless_spark_session() -> DatabricksSession:
    return DatabricksSession.builder.serverless().getOrCreate()
