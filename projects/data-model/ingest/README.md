# Data Model Development: Ingest
Ingest refers to fetching data from the source databases and other source systems. Each source system has a notebook for ingest under the ingest folder. Changes to ingestion can be done from the Databricks Dev Workspace or in Visual Studio Code.

## Databricks Dev Workspace
1. Go into sous chef from Databricks
2. Pull changes from main and create a new branch
3. Open the relevant ingest notebook (`bronze_<source_system>_full`)
4. Add the tables you want to include in the tables list in alphabetic order
5. Commit and push changes from Databricks
6. Create a PR and assign Marie or Anna to review

To ingest the data immediatly to bronze you can run the notebook only for the tables that you have added by either commenting out the other tables or copy the cell and remove the tables that already exist.

## Visual Studio Code
1. Ensure that you are in main: ```git checkout main```
2. Pull the latest changes from main: ```git pull```
3. Create a new branch: ```git checkout -b "firstname/your-branch-name"```
3. Open the relevant ingest notebook (`bronze_<source_system>_full`)
4. Add the tables you want to include in the tables list in alphabetic order
6. Commit and push changes to GitHub (ask if you do not how)
7. Create a PR and assign Marie or Anna to review
