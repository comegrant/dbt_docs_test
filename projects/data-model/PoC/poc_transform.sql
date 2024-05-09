-- Databricks notebook source
-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM BILLING AGREEMENT

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocsilver.cms_billing_agreement (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  agreement_id INT NOT NULL,
  company_id CHAR(36) NOT NULL,
  status_id INT NOT NULL,
  start_date DATE NOT NULL, -- How to deal with start date?
  source VARCHAR(126)
);

-- COMMAND ----------

INSERT INTO
  dev.temppocsilver.cms_billing_agreement(
    agreement_id,
    company_id,
    status_id,
    start_date,
    source
  )
SELECT
  agreement_id,
  company_id,
  status,
  start_date,
  source
FROM
  dev.temppocbronze.cms_billing_agreement

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocsilver.cms_billing_agreement_status (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  status_id INT NOT NULL,
  status_description VARCHAR(50) NOT NULL
);

-- COMMAND ----------

INSERT INTO
  dev.temppocsilver.cms_billing_agreement_status (status_id, status_description)
SELECT
  status_id,
  status_description
FROM
  dev.temppocbronze.cms_billing_agreement_status

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocgold.dim_billing_agreement (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  agreement_id INT NOT NULL,
  status VARCHAR(50) NOT NULL,
  start_date TIMESTAMP NOT NULL,
  source VARCHAR(126)
);

-- COMMAND ----------

INSERT INTO
  dev.temppocgold.dim_billing_agreement(
    agreement_id,
    status,
    start_date,
    source
  )
SELECT
  cba.agreement_id,
  cbas.status_description AS status,
  cba.start_date,
  cba.source
FROM
  dev.temppocsilver.cms_billing_agreement cba
  LEFT JOIN dev.temppocsilver.cms_billing_agreement_status cbas ON cba.status_id = cbas.status_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Notater
-- MAGIC - Hvordan forhold seg til start_date for kunder?
-- MAGIC - Kanskje ha en fact subscription?
-- MAGIC - F.eks. antall kunder på en adresse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM BRAND

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocsilver.cms_company(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  company_id CHAR(36) NOT NULL,
  company_name VARCHAR(512) NOT NULL
)

-- COMMAND ----------

INSERT INTO
  dev.temppocsilver.cms_company (company_id, company_name)
SELECT
  id,
  company_name
FROM
  dev.temppocbronze.cms_company
WHERE
  billing_address_country IS NOT NULL

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocgold.dim_company(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  company_id CHAR(36) NOT NULL,
  company_name VARCHAR(512) NOT NULL
)

-- COMMAND ----------

INSERT INTO
  dev.temppocgold.dim_company(
	company_id, 
	company_name
	)
SELECT
  company_id, 
	company_name
FROM
  dev.temppocsilver.cms_company

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM CUSTOMER ADRESS

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocsilver.cms_address(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
	agreement_id INT NOT NULL,
  agreement_address_id CHAR(36) NOT NULL,
  address_id VARCHAR(32),
  postal_code VARCHAR(256) NOT NULL,
  country VARCHAR(32) NOT NULL,
  city VARCHAR(64) NOT NULL,
  street VARCHAR(256) NOT NULL
)

-- COMMAND ----------

INSERT INTO
  dev.temppocsilver.cms_address(
    agreement_id,
    agreement_address_id,
    address_id,
    postal_code,
    country,
    city,
    street
  )
SELECT
  agreement_id,
  id,
  MD5(CONCAT(postal_code, country, city, street)),
  postal_code,
  country,
  city,
  street
FROM
  dev.temppocbronze.cms_address_live
UNION
SELECT
  agreement_id,
  id,
  MD5(CONCAT(postal_code, country, city, street)),
  postal_code,
  country,
  city,
  street
FROM
  dev.temppocbronze.cms_address_history

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocgold.dim_address(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  address_id VARCHAR(32),
  postal_code VARCHAR(256) NOT NULL,
  country VARCHAR(32) NOT NULL,
  city VARCHAR(64) NOT NULL,
  street VARCHAR(256) NOT NULL
)

-- COMMAND ----------

INSERT INTO
  dev.temppocgold.dim_address(
		  address_id,
      postal_code,
      country,
      city,
      street
	)
    SELECT
      address_id,
      postal_code,
      country,
      city,
      street
    FROM
      dev.temppocsilver.cms_address

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM CALENDAR

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import sys
-- MAGIC
-- MAGIC packages = ["../", "../../../packages/time-machine/"]
-- MAGIC sys.path.extend(packages)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(sys.path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from time_machine.holidays import *
-- MAGIC
-- MAGIC dates = get_calendar_dataframe_with_holiday_features("2022-01-01", "2025-12-31", "Norway")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dates.head()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dates = dates[['date', 'year', 'week']]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dates = spark.createDataFrame(dates)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dates.write.mode("overwrite").saveAsTable("dev.temppocgold.dim_date")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIM PRODUCT VARIATION

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocsilver.product_layer_product_variation(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  product_id CHAR(36) NOT NULL,
  product_name VARCHAR(255) NOT NULL,
  product_type_id CHAR(36) NOT NULL,
  product_type_name VARCHAR(255) NOT NULL,
  variation_id CHAR(36) NOT NULL,
  variation_sku VARCHAR(255) NOT NULL,
  variation_name VARCHAR(255) NOT NULL,
  company_id CHAR(36) NOT NULL,
  variation_meals INT,
  variation_portions INT,
  product_concept_id CHAR(36),
  product_concept_name VARCHAR(255)
)

-- COMMAND ----------

INSERT INTO
  dev.temppocsilver.product_layer_product_variation(
    product_id,
    product_name,
    product_type_id,
    product_type_name,
    variation_id,
    variation_sku,
    variation_name,
    company_id,
    variation_meals,
    variation_portions,
    product_concept_id,
    product_concept_name
  )
    SELECT
      p.id AS product_id,
      p.name AS product_name,
      pt.product_type_id,
      pt.product_type_name,
      pv.id AS variation_id,
      pv.sku AS variation_sku,
      pvc.name AS variation_name,
      pvc.company_id AS variation_company_id,
      TRY_CAST(
        COALESCE(pvav.attribute_value, pvat.default_value) AS INT
      ) AS variation_meals,
      TRY_CAST(
        (
          CASE
            WHEN pvc.company_id = '5e65a955-7b1a-446c-b24f-cfe576bf52d7'
            AND (
              pvc.name like '%2+%'
              OR pvc.name like '%3+%'
              OR pvc.name like '%4+%'
            ) THEN LEFT(
              COALESCE(pvav2.attribute_value, pvat2.default_value),
              1
            )
            ELSE COALESCE(pvav2.attribute_value, pvat2.default_value)
          END
        ) AS INT
      ) AS variation_portions,
      pc.product_concept_id,
      pc.product_concept_name
    FROM
      dev.temppocbronze.product_layer_product p
      INNER JOIN dev.temppocbronze.product_layer_product_type pt ON pt.product_type_id = p.product_type_id
      INNER JOIN dev.temppocbronze.product_layer_product_variation pv ON pv.product_id = p.id
      INNER JOIN dev.temppocbronze.product_layer_product_variation_company pvc ON pvc.variation_id = pv.id
      LEFT JOIN dev.temppocbronze.product_layer_product_type_concept ptc ON ptc.product_type_id = p.product_type_id
      LEFT JOIN dev.temppocbronze.product_layer_product_concept pc ON pc.product_concept_id = ptc.product_concept_id
      LEFT JOIN dev.temppocbronze.product_layer_product_variation_attribute_value pvav ON pvav.variation_id = pvc.variation_id
      AND pvav.company_id = pvc.company_id
      AND pvav.attribute_id IN (
        'EDF04536-BC72-41FC-9491-024DD3E48FFF',
        '04974035-6A0A-4BC0-87C6-11138BB5B08F',
        '462A097B-2B57-4CD0-AF38-26BAC99B6020',
        '4FF0F041-CEEB-4DBB-B191-7C2F3BFEF225',
        'E69BB4A1-94A4-4AE3-B527-89A51B70D613',
        'DBE23C41-F6AF-490B-9ED1-B57B74340500',
        '4D4F9B2C-A876-4A1F-B17C-F4500F577202',
        '9221B4A2-2647-42D7-9E25-F97D0023A2B7',
        '8BA21161-51AC-4794-9FFA-F9EB2BDC11E0',
        '2F3B3E18-82D3-48A8-9DC6-DCDA96AA4EEC'
      ) -- MEALS
      LEFT JOIN dev.temppocbronze.product_layer_product_variation_attribute_template pvat ON pvat.product_type_id = p.product_type_id
      AND pvat.attribute_id IN (
        'EDF04536-BC72-41FC-9491-024DD3E48FFF',
        '04974035-6A0A-4BC0-87C6-11138BB5B08F',
        '462A097B-2B57-4CD0-AF38-26BAC99B6020',
        '4FF0F041-CEEB-4DBB-B191-7C2F3BFEF225',
        'E69BB4A1-94A4-4AE3-B527-89A51B70D613',
        'DBE23C41-F6AF-490B-9ED1-B57B74340500',
        '4D4F9B2C-A876-4A1F-B17C-F4500F577202',
        '9221B4A2-2647-42D7-9E25-F97D0023A2B7',
        '8BA21161-51AC-4794-9FFA-F9EB2BDC11E0',
        '2F3B3E18-82D3-48A8-9DC6-DCDA96AA4EEC'
      ) -- MEALS
      LEFT JOIN dev.temppocbronze.product_layer_product_variation_attribute_value pvav2 ON pvav2.variation_id = pvc.variation_id
      AND pvav2.company_id = pvc.company_id
      AND pvav2.attribute_id IN (
        'B438F8A6-3E2A-4C40-A930-0093FE84EC75',
        '0AB58A86-6B14-429C-B9A0-4A4D17132C0F',
        '9CABEACD-B730-40DD-81FE-6092B7FAB066',
        'B1425946-B5A7-4D81-A5F7-6F92863009BD',
        'C4205C73-44B5-4F88-BB85-8F56C2CB92B1',
        'E310C663-8226-4B16-8A7A-91C12309FEE6',
        '8DB1BF15-9D1B-492F-B69E-C7A37F95FEEF',
        'D376B880-C630-434C-9EB3-E4009DBBCF8C',
        'B1BEE06A-36AE-418F-B493-EEAB35B7BE3E',
        'F52FC09F-AC4C-46B6-B9CE-9881BCC49743'
      ) -- PORTIONS
      LEFT JOIN dev.temppocbronze.product_layer_product_variation_attribute_template pvat2 ON pvat2.product_type_id = p.product_type_id
      AND pvat2.attribute_id IN (
        'B438F8A6-3E2A-4C40-A930-0093FE84EC75',
        '0AB58A86-6B14-429C-B9A0-4A4D17132C0F',
        '9CABEACD-B730-40DD-81FE-6092B7FAB066',
        'B1425946-B5A7-4D81-A5F7-6F92863009BD',
        'C4205C73-44B5-4F88-BB85-8F56C2CB92B1',
        'E310C663-8226-4B16-8A7A-91C12309FEE6',
        '8DB1BF15-9D1B-492F-B69E-C7A37F95FEEF',
        'D376B880-C630-434C-9EB3-E4009DBBCF8C',
        'B1BEE06A-36AE-418F-B493-EEAB35B7BE3E',
        'F52FC09F-AC4C-46B6-B9CE-9881BCC49743'
      ) -- PORTIONS
  

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocgold.dim_product_variation(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  variation_id CHAR(36) NOT NULL,
  variation_sku VARCHAR(255) NOT NULL,
  variation_name VARCHAR(255) NOT NULL,
  variation_meals INT,
  variation_portions INT,
  product_name VARCHAR(255) NOT NULL,
  product_type_name VARCHAR(255) NOT NULL,
  product_concept_name VARCHAR(255)
)

-- COMMAND ----------

INSERT INTO
  dev.temppocgold.dim_product_variation (
    variation_id,
    variation_sku,
    variation_name,
    variation_meals,
    variation_portions,
    product_name,
    product_type_name,
    product_concept_name
  )
SELECT
  variation_id,
  variation_sku,
  variation_name,
  variation_meals,
  variation_portions,
  product_name,
  product_type_name,
  product_concept_name
FROM
  dev.temppocsilver.product_layer_product_variation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FACT ORDER LINE

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocsilver.cms_billing_agreement_order_line(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  order_line_id CHAR(36) NOT NULL,
  order_id CHAR(36) NOT NULL,
  variation_id CHAR(36),
  variation_qty INT,
  price NUMERIC(20, 6),
  vat INT,
  line_type CHAR(25)
)

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.temppocsilver.cms_billing_agreement_order(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  order_id CHAR(36) NOT NULL,
  agreement_id INT NOT NULL,
  company_id CHAR(36) NOT NULL,
  address_id CHAR(36),
  order_number BIGINT,
  created_date TIMESTAMP NOT NULL,
  has_recipe_leaflets BOOLEAN
)

-- COMMAND ----------

INSERT INTO
  dev.temppocsilver.cms_billing_agreement_order_line(
    order_line_id,
    order_id,
    variation_id,
    variation_qty,
    price,
    vat,
    line_type
  )
SELECT
  id,
  agreement_order_id,
  variation_id,
  variation_qty,
  price,
  vat,
  typeOfLine
FROM
  dev.temppocbronze.cms_billing_agreement_order_line

-- COMMAND ----------

INSERT INTO
  dev.temppocsilver.cms_billing_agreement_order(
    order_id,
    agreement_id,
    company_id,
    address_id,
    order_number,
    created_date,
    has_recipe_leaflets
  )
SELECT
  bao.id,
  bao.agreement_id,
  ba.company_id,
  a.address_id,
  bao.order_id,
  bao.created_date,
  bao.has_recipe_leaflets
FROM
  dev.temppocbronze.cms_billing_agreement_order bao
LEFT JOIN dev.temppocsilver.cms_billing_agreement ba ON bao.agreement_id = ba.agreement_id
LEFT JOIN dev.temppocsilver.cms_address a ON bao.shipping_address_id = a.agreement_address_id


-- COMMAND ----------

CREATE
OR REPLACE TABLE dev.temppocgold.fact_order_line(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  order_id CHAR(36) NOT NULL,
  order_number BIGINT,
  order_line_id CHAR(36) NOT NULL,
  variation_qty INT,
  price NUMERIC(20, 6),
  vat INT,
  line_type CHAR(25),
  created_date DATE NOT NULL,
  has_recipe_leaflets BOOLEAN,
  fk_dim_company BIGINT,
  fk_dim_billing_agreement BIGINT,
  fk_dim_address BIGINT,
  fk_dim_product_variation BIGINT
)

-- COMMAND ----------

INSERT INTO
  dev.temppocgold.fact_order_line (
    order_id,
    order_number,
    order_line_id,
    variation_qty,
    price,
    vat,
    line_type,
    created_date,
    has_recipe_leaflets,
    fk_dim_company,
    fk_dim_billing_agreement,
    fk_dim_address,
    fk_dim_product_variation
  )
SELECT
  bao.order_id,
  bao.order_number,
  baol.order_line_id,
  baol.variation_qty,
  baol.price,
  baol.vat,
  baol.line_type,
  bao.created_date,
  bao.has_recipe_leaflets,
  dim_co.id AS fk_dim_company,
  dim_ba.id AS fk_dim_billing_agreement,
  dim_ad.id AS fk_dim_address,
  dim_pv.id AS fk_dim_product_variation
FROM
  dev.temppocsilver.cms_billing_agreement_order_line baol
  LEFT JOIN dev.temppocsilver.cms_billing_agreement_order bao ON baol.order_id = bao.order_id
  LEFT JOIN dev.temppocgold.dim_company dim_co ON bao.company_id = dim_co.company_id
  LEFT JOIN dev.temppocgold.dim_billing_agreement dim_ba ON bao.agreement_id = dim_ba.agreement_id
  LEFT JOIN dev.temppocgold.dim_address dim_ad ON bao.address_id = dim_ad.address_id
  LEFT JOIN dev.temppocgold.dim_product_variation dim_pv ON baol.variation_id = dim_pv.variation_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FACT SURVEY FREEZE REASON

-- COMMAND ----------

CREATE
OR REPLACE TABLE dev.temppocgold.fact_freezed_subscription_survey(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  original_timestamp TIMESTAMP,
  anonymous_id CHAR(36),
  where_will_you_eat STRING,
  freeze_reason_comment STRING,
  main_freeze_reason STRING,
  sub_freeze_reason STRING,
  fk_dim_company BIGINT,
  fk_dim_billing_agreement BIGINT
)

-- COMMAND ----------

INSERT INTO dev.temppocgold.fact_freezed_subscription_survey(
    original_timestamp
    ,anonymous_id
    ,where_will_you_eat
    ,freeze_reason_comment
    ,main_freeze_reason
    ,sub_freeze_reason
    ,fk_dim_company
    ,fk_dim_billing_agreement
)
SELECT
    original_timestamp
    ,anonymous_id
    ,where_will_you_eat
    ,freeze_reason_comment
    ,main_freeze_reason
    ,sub_freeze_reason
    ,dim_co.id AS fk_dim_company
    ,COALESCE(dim_ba.id, '-1') AS fk_dim_billing_agreement
  FROM dev.temppocsilver.fact_freezed_subscription_survey ffss
  LEFT JOIN dev.temppocgold.dim_company dim_co ON ffss.company_id = dim_co.company_id
  LEFT JOIN dev.temppocgold.dim_billing_agreement dim_ba ON ffss.user_id = dim_ba.agreement_id
