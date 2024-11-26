# About the project

This projects holds code for the ETL pipelines of the Data Platform. We use [Databricks Notebooks](https://docs.databricks.com/en/notebooks/index.html) for ingestion, [dbt](https://docs.getdbt.com/) for transformations and [Databricks Asset Bundles (DAB)](https://docs.databricks.com/en/dev-tools/bundles/index.html) for orchestration and deployment of the code to Databricks.

# Folder Structure

**data-model/ingest**: Code related to ingestion of source data using notebooks.

**data-model/resources**: Code for setting up workflows using DABs.

**data-model/transform**: dbt project files for transforming and modelling the data.

Each folder has its own README.md please read this for more information. 
