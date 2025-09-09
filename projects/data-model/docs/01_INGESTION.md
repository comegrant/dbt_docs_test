# Data Ingestion Guide

This document provides guidance on the data ingestion processes. Ingestion refers to fetching data from the source databases and other source systems and storing them in the bronze layer in Databricks.

To ingest data, we use python notebooks in Databricks and helper functions which connect the notebook to the source system. Each notebook is scheduled to be run on a regular basis using Databricks Workflows.

## Table of Contents

1. [🚀 Development Steps](#-1-development-steps)
2. [📥 Ingestion Types](#-2-ingestion-types)
   - [2.1 Full ingest](#21-full-ingest)
   - [2.2 Incremental ingest](#22-incremental-ingest)
   - [2.3 Snapshot ingest](#23-snapshot-ingest)
   - [2.4 Selected Columns ingest](#24-selected-columns-ingest)
   - [2.5 One-Time ingest](#25-one-time-ingest)

## 🚀 1. Development Steps

1. Follow the guidelines for each [ingestion type](#ingestion-types)
2. Ingest the new data to bronze in dev
    - Go into the Databricks workspace
    - Go into sous-chef from Databricks (need to connect to GitHub if you haven't)
    - Find the branch with the updated ingestion code
    - Make temporary modifications to the file to run only the newly added tables
3. If creating a new ingestion script, add the script to the ingestion job and other relevant jobs found in the jobs-folder.

## 📥 2. Ingestion Types
There are different types of ingestion notebooks to be used depending on the type of table that is added.
- **Full ingest:** Complete data refresh of all columns with every ingest.
- **Incremental ingest:** Ingest the most recent data only.
- **Snapshot ingest:** Ingest of tables that we take snapshots of to preserve history.
- **Selected Columns ingest:** Ingest with explicit selection of columns from the source.
- **One-time ingest:** Ingest that is only performed once / manually.

Each ingestion type is described further in the sections below.

---

### 2.1 Full ingest
When performing a full ingest, new data from the source is added by overwriting the existing data in the data platform.

📁 Folder: projects/data-model/ingest/full_ingest/

⏱️ Schedule: Once per day

**When to use:**
- For tables that are not very big and fast growing.
- All columns can be added without concern for senstive data or GDPR.
- Deleted rows in the source table does not need to be kept.

**How to use:**
- Find the notebook that belongs to the source system you want to ingest data from.
- Add table to the list of tables in the notebook in alphabetical order.

---

### 2.2 Incremental ingest
For very big tables, it costs a lot of computer power to reload the table every time new data is being extracted. Hence, we rather perform an incremental ingest. When performing an incremental ingest, we extract only the newest rows from the source.

📁 Folder: projects/data-model/ingest/incremental_ingest/

⏱️ Schedule: Once per day

**When to use:**
- Table is big and gets large amounts of new data frequently
- Historical rows in the source table does not get updated far back in time (>12 months).

**How to use:**
- Create custom ingest script only extracting data from X days before the last sync.
- Use example script to start with: `layer_sourcename_incremental_ingest_example`

---

### 2.3 Snapshot ingest
For some tables, historic data is not stored in the source system. However, the historic data can be important for analysis purposes. In this case, we want to extract new data more often than for regular full ingest to be able to track changes to the table within the day.

📁 Folder: projects/data-model/ingest/snapshot_ingest/

⏱️ Schedule: Every 2 hours during daytime

**When to use:**
- If history is not kept in the source data, but is needed for analytics purposes.

**How to use:**
- Add table to `bronze_coredb_snapshot_full` under the correct source system.
- Or create a custom script if needed.

---

### 2.4 Selected Columns ingest
📁 Folder: projects/data-model/ingest/selected_column_ingest/

⏱️ Schedule: Once per day

When performing a regular ingest, we automatically extract all columns. Sometimes we want to exclude columns in the source system (e.g., due to GDPR). In that case, we have to specify which columns to ingest. The ingest can be either full or incremental depending on the size of the table.

**When to use:**
- If not all columns should be extracted from the source.

**How to use:**
- Create custom ingest script only ingesting specific columns.
- Use example file as basis: `layer_sourcesystem_example_ingest`

---

### 2.5 One-Time ingest
Some tables do not need to be ingested on a regular basis. This is typically historic tables where no new data is added moving forward. In this case, we only need to ingest the data once. 

📁 Folder: projects/data-model/ingest/onetime_ingest/

⏱️ Schedule: None

**When to use:**
- For tables that will not be updated in the future (example: Calendar Table, history from the old Analytics Database)

**How to use:**
- Add table to the already existing notebooks.
- Or create a custom script if needed.