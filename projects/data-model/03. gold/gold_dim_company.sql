-- Databricks notebook source
CREATE OR REPLACE TABLE dev.gold.dim_company
(
  dim_company_id STRING,
  company_name STRING,
  country_name STRING,
  country_shortname STRING,
  currency STRING,
  loaded_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP
)
USING DELTA

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW v_company AS
SELECT
  ID AS company_id,
  COMPANY_NAME AS company_name,
  country_id
FROM
  dev.bronze.cms_company

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW v_country AS
SELECT
  id as country_id,
  name as country_name,
  shortname as country_shortname,
  currency
FROM
  dev.bronze.cms_country

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW v_dim_company AS
SELECT
  company_id,
  company_name,
  country_name,
  country_shortname,
  currency
FROM
  v_company AS cmp
  LEFT JOIN v_country AS ctr ON cmp.country_id = ctr.country_id

-- COMMAND ----------

MERGE INTO dev.gold.dim_company AS target USING v_dim_company AS source ON target.dim_company_id = source.company_id
WHEN MATCHED THEN
UPDATE
SET
  target.company_name = source.company_name,
  target.country_name = source.country_name,
  target.country_shortname = source.country_shortname,
  target.currency = source.currency,
  target.updated_timestamp = current_timestamp()
  WHEN NOT MATCHED THEN
INSERT
  (
    dim_company_id,
    company_name,
    country_name,
    country_shortname,
    currency,
    loaded_timestamp,
    updated_timestamp
  )
VALUES
  (
    source.company_id,
    source.company_name,
    source.country_name,
    source.country_shortname,
    source.currency,
    current_timestamp(),
    current_timestamp()
  )
