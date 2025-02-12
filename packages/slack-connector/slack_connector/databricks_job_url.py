def get_databricks_job_url(mode: str = "workflow_job") -> str:
    """
    Constructs and returns the URL for the current Databricks job run.

    Retrieves the correct ids to construct the URL from the Spark session.
    If running embedded in a notebook or python script use "embedded".
    If running in a workflow job where DABs are used, use "workflow_job".

    Args:
        mode (str): The mode of the job run. Accepts "workflow_job" or "embedded". Default is "workflow_job".

    Returns:
        str: The URL to the Databricks job run.
    """
    # this function assumes you are running in a dataricks runtime that comes preinstalled with Apache Spark
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()

    job_id = spark.sparkContext.getLocalProperty("spark.databricks.job.id")
    host_url = spark.conf.get("spark.databricks.workspaceUrl")
    if mode != "embedded":
        run_id = spark.sparkContext.getLocalProperty("spark.databricks.job.runId")  # Gives Task runID
        url_to_job = f"{host_url}/jobs/{job_id}/runs/{run_id}"

    else:  # Default case is "workflow_job"
        parent_run_id = spark.sparkContext.getLocalProperty("spark.databricks.job.parentRunId")
        url_to_job = f"{host_url}/jobs/{job_id}/runs/{parent_run_id}"

    return url_to_job


if __name__ == "__main__":
    print(get_databricks_job_url("embedded"))  # noqa: T201
