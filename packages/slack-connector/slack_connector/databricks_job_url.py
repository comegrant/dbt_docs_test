def get_databricks_job_url() -> str:
    """
    Constructs and returns the URL for the current Databricks job run.

    Retrieves the job ID and parent run ID from the Spark context's local properties
    and the workspace URL from the Spark configuration to build the complete URL
    pointing to the specific job run in Databricks.

    Returns:
        str: The URL to the Databricks job run.
    """
    # this function assumes you are running in a dataricks runtime that comes preinstalled with Apache Spark
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()

    job_id = spark.sparkContext.getLocalProperty("spark.databricks.job.id")
    parent_run_id = spark.sparkContext.getLocalProperty("spark.databricks.job.parentRunId")
    host_url = spark.conf.get("spark.databricks.workspaceUrl")
    url_to_job = f"{host_url}/jobs/{job_id}/runs/{parent_run_id}"

    return url_to_job
