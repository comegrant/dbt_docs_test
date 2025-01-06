# Ingest
Ingest refers to fetching data from the source databases and other source systems and store them in the bronze layer in Databricks To ingest data we use python notebooks and helper functions which connects the notebook to the source system. Each notebook is scheduled to be run on a regular basis using Databricks Workflows which ensures that the data is up to date.

There are different types of ingest notebooks to be used depending on the type of table that is added.
1. **Full ingest:** Reloads the full table every day.
2. **Incremental ingest:** Ingest the most recent data only.
3. **Snapshot ingest:** Ingest performed every 2h hour to track history.
4. **One time ingest:** Ingest that are only performed once.
5. **Selected Columns ingest:** Ingest with explicit selection of columns from the source.

Each ingestion type is described futher in the sections below.

## General Full ingest
📁 projects/data-model/ingest/full_ingest/
⏱️ Schedule: Once per day

When performing a full ingest new data from the source is added by overwriting the existing data in the data plattfrom.

**When to use:**
* For tables with less than 1 million rows
* History is kept in the source table are not relevant

**How to use:**
* Find the notebook that belongs to the source system you want to ingest data from. 
* Add table to the list of tables in the Notebook in alphabetic order.

## General Incremental ingest
📁 projects/data-model/ingest/incremental_ingest/
⏱️ Schedule: Once per day

For very big tables (> ~1M rows) it costs a lot of computer power to reload the table everytime new data is being extracted. Hence, we rather perform an incremental ingest which means that we only extract the newest rows and append those to the tables in silver. Currently we use a strategy were we only extract data from the past X number of days.

**When to use:**
* Table has more than 1 milliion rows.
* Old rows in the table will not get updated

**How to use:**
* Create custom ingest script only extracting data X days back in time.
* Look at existing scripts for examples.

## Snapshot ingest
📁 projects/data-model/ingest/snapshot_ingest/
⏱️ Schedule: Every 2nd hour in day time

For some tables historic data is not stored in the source system. However, the historic data can be important for analysis purposes. In this case we want to extract new data more often than for regular full ingest to be able to track changes to the table within the day.

**When to use:**
* If history is not kept in the source data, but needed for analytics purposes.

**How to use:**
* Add table to the list in the notebook under the correct source system.
* Create a custom script if needed.

## One time ingest
📁 projects/data-model/ingest/onetime_ingest/
⏱️ Schedule: None

Some tables does not need to be ingested on a regular basis. This is typically historic tables where no new data is added moving forward. In this case we only need to ingest the data once. 

**When to use:**
* For tables that will not be updated in the future.

**How to use:**
* Add a cell with schema and table name to the notebook for the correct source system. (Follow the same structure as found in the notebook).

## Selected Columns ingest
📁 projects/data-model/ingest/selected_column_ingest/
⏱️ Schedule: One time per day

When performing a regular ingest we automatically extract all columns. Sometimes we want to exclude columns in the source system (e.g., due to GDPR). In that case we have to specify which columns to ingest. The ingest can both be full or incremental depending on the size of the table.

**When to use:**
* If not all columns should be extracted from the source. 

**How to use:**
* Create custom ingest script only ingesting specific columns.
* Look at existing scripts for examples.