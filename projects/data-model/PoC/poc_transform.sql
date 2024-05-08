-- Databricks notebook source
-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM BILLING AGREEMENT

-- COMMAND ----------

CREATE TABLE dev.poc_silver.cms_billing_agreement
(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  agreement_id  INT NOT NULL,
  company_id  UNIQUEIDENTIFIER NOT NULL,
  status_id INT NOT NULL,
  start_date DATETIME NOT NULL,
  source NVARCHAR(126) NULL
);

-- COMMAND ----------

CREATE TABLE dev.poc_silver.cms_billing_agreement_status
(
	id BIGINT GENERATED ALWAYS AS IDENTITY,
	status_id INT NOT NULL,
	status_description VARCHAR(50) NOT NULL
);

-- COMMAND ----------

INSERT INTO dev.poc_silver.cms_billing_agreement
SELECT 
  agreement_id,
  company_id
  status,
  start_date,
  source,
FROM dev.poc_bronze.cms_billing_agreement

-- COMMAND ----------

INSERT INTO dev.poc_silver.cms_billing_agreement_status
SELECT 
  status_id,
  status_description
FROM dev.poc_bronze.cms_billing_agreement_status

-- COMMAND ----------

CREATE TABLE dev.poc_gold.dim_billing_agreement
(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  agreement_id  INT NOT NULL,
  status VARCHAR(50) NOT NULL,
  start_date DATETIME NOT NULL,
  source NVARCHAR(126) NULL
);

-- COMMAND ----------

INSERT INTO dev.poc_gold.dim_billing_agreement
SELECT 
  cba.agreement_id,
  cba.company_id
  cbas.status_description AS status,
  cba.start_date,
  cba.source,
FROM dev.poc_silver.cms_billing_agreement cba
LEFT JOIN dev.poc_silver.cms_billing_agreement_status cbas
ON cba.status_id = cbas.status_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM BRAND

-- COMMAND ----------

CREATE TABLE dev.poc_silver.cms_company(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
	company_id UNIQUEIDENTIFIER NOT NULL,
	company_name NVARCHAR(512) NOT NULL,
	cutoff_day_time TIME(7) NULL,
	cutoff_day INT NOT NULL,
	billing_address_country NVARCHAR(128) NULL
)

-- COMMAND ----------

INSERT INTO dev.poc_silver.cms_company
SELECT 
  company_id
  company_name,
	cutoff_day_time,
	cutoff_day,
	billing_address_country
FROM dev.poc_bronze.cms_company
WHERE billing_adress_country IS NOT NULL

-- COMMAND ----------

CREATE TABLE dev.poc_gold.dim_company(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
	company_id UNIQUEIDENTIFIER NOT NULL,
	company_name NVARCHAR(512) NOT NULL,
	cutoff_day_time TIME(7) NULL,
	cutoff_day INT NOT NULL,
	billing_address_country NVARCHAR(128) NULL
)

-- COMMAND ----------

INSERT INTO dev.poc_gold.dim_company
SELECT 
  company_id
  company_name,
	cutoff_day_time,
	cutoff_day,
	billing_address_country
FROM dev.poc_silver.cms_company

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM CUSTOMER ADRESS

-- COMMAND ----------

CREATE TABLE dev.poc_silver.cms_adress(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  shipping_address_id UNIQUEIDENTIFIER NOT NULL,
	address_id NVARCHAR(32),
	agreement_id INT NOT NULL,
	postal_code NVARCHAR(256) NOT NULL,
	country NVARCHAR(32) NOT NULL,
	city NVARCHAR(64) NOT NULL,
	alias NVARCHAR(256) NULL,
	street NVARCHAR(256) NOT NULL
)

-- COMMAND ----------

INSERT INTO dev.poc_silver.cms_address(
SELECT 
  	shipping_address_id,
	MD5(CONCAT(postal_code, country, city, street))
	agreement_id,
	postal_code,
	country,
	city,
	alias,
	street
FROM dev.poc_bronze.cms_address_live

UNION

SELECT 
  shipping_address_id,
	MD5(CONCAT(postal_code, country, city, street)),
	agreement_id,
	postal_code,
	country,
	city,
	alias,
	street
FROM dev.poc_bronze.cms_address_history)

-- COMMAND ----------

CREATE TABLE dev.poc_gold.dim_adress(
  	id BIGINT GENERATED ALWAYS AS IDENTITY,
  	address_id NVARCHAR(32),
	postal_code NVARCHAR(256) NOT NULL,
	country NVARCHAR(32) NOT NULL,
	city NVARCHAR(64) NOT NULL,
	street NVARCHAR(256) NOT NULL
)

-- COMMAND ----------

INSERT INTO dev.poc_gold.dim_address(
SELECT 
  address_id,
	agreement_id,
	postal_code,
	country,
	city,
	alias,
	street
FROM dev.poc_silver.cms_address)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM DATE

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC pwd

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC ls ../../../packages

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import sys
-- MAGIC packages = [
-- MAGIC     "../",
-- MAGIC     "../../../packages/constants//"
-- MAGIC ]
-- MAGIC sys.path.extend(packages)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print(sys.path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from constants.companies import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from packages.time-machine.time_machine.holidays import get_calendar_dataframe_with_holiday_features
-- MAGIC
-- MAGIC dates_norway = get_calendar_dataframe_with_holiday_features("2022-01-01", "2025-12-31", "Norway")
-- MAGIC
-- MAGIC dates_norway.head()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM PRODUCT VARIATION

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FACT ORDER LINE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FACT SURVEY FREEZE REASON

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC pwd

-- COMMAND ----------


