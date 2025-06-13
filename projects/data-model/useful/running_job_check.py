"""
This script is used to check what jobs are running in the Databricks workspace.
It is used as a dependency check in other jobs.
"""

import argparse
import json
import logging
import sys 
import requests
from typing import Any, Literal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)



def get_running_jobs(
    environment: Literal["dev", "test", "prod"] = "dev",
    job_name: str | None = None
) -> list[dict[str, Any]]:
    """
    Returns a list of running job instances. If job_name is provided, only returns jobs matching that name.

    Args:
        environment: The environment to check for (dev, test, prod). Defaults to "dev".
        job_name: Optional name of the job to filter for. If None, returns all running jobs.
    Returns:
        List of running job instances. If job_name is provided, only returns jobs matching that name.
    """
    logger.info("🛍️ Fetching running jobs for environment=%s", environment)
        
    if job_name:
        logger.info("Filtering for job_name=%s", job_name)

    # Get Databricks instance and access token
    databricks_instance = spark.conf.get("spark.databricks.workspaceUrl")
    access_token = dbutils.secrets.get(scope="auth_common", key=f"databricks-sp-bundle-pat-{environment}")

    # Set the API endpoint
    url = f"https://{databricks_instance}/api/2.0/jobs/runs/list?active_only=true"
    logger.debug("Using API endpoint: %s", url)

    # Set the headers
    headers = {"Authorization": f"Bearer {access_token}"}

    # Make the GET request
    logger.debug("Making GET request to Databricks API")
    response = requests.get(url, headers=headers)

    # Add bundle_sp_dev id to job name if in dev environment and job_name is provided
    # TODO: I'm pretty sure this could be done better, but I don't have a lot of time to do it right now.
    if environment == "dev" and job_name:
        job_name = f"[dev bundle_sp_dev] {job_name}"
        logger.debug("Modified job name for dev environment: %s", job_name)

    # Check if the request was successful
    if response.status_code == 200:
        running_jobs = response.json()
        if job_name:
            filtered_jobs = [job for job in running_jobs.get("runs", []) if job.get("run_name") == job_name]
            logger.info("🔍 Found %d running instances of job %s", len(filtered_jobs), job_name)
            return filtered_jobs
        else:
            all_jobs = [job for job in running_jobs.get("runs", []) if "running-job-check" not in job.get("run_name", "")]
            logger.info("🔍 Found %d total running jobs (excluding 'running-job-check')", len(all_jobs))
            return all_jobs
    else:
        error_msg = f"Error: {response.status_code}, {response.text}"
        logger.error("❌ %s", error_msg)
        raise Exception(error_msg)

def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job_name",
        type=str,
        required=False,
        help="The name of the job to check for",
    )
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["dev", "test", "prod"],
        required=True,
        help="Environment (dev, test, prod)",
    )

    args = parser.parse_args()
    environment = args.environment
    job_name = args.job_name

    if job_name == "":
        job_name = None

    return environment, job_name


def main():
    """
    Main function to check if a job is running in the Databricks workspace.
    """
    environment, job_name = parse_args()
    logger.info("🚀 Starting job check with arguments: %s %s", environment, job_name)
    try:
        running_jobs = get_running_jobs(
            job_name=job_name, environment=environment
        )

        if running_jobs:
            if job_name:
                error_msg = f"Job '{job_name}' is currently running in {len(running_jobs)} instances. This job will now fail accordingly."
                logger.error("⚠️ %s", error_msg)
            else:
                error_msg = f"Found {len(running_jobs)} jobs currently running. This job will now fail accordingly."
                logger.error("⚠️ %s", error_msg)
            logger.error("📋 Running job details: %s", json.dumps(running_jobs, indent=4))
            sys.exit(1)
        else:
            if job_name:
                logger.info("✅ Job '%s' is not running. This job will now succeed.", job_name)
            else:
                logger.info("✅ No jobs are currently running. This job will now succeed.")

    except Exception as e:
        logger.exception("💥 Error checking job status: %s", str(e))
        sys.exit(1)  # Exit with failure status


if __name__ == "__main__":
    main()
