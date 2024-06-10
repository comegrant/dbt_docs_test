-- Databricks notebook source
CREATE TABLE IF NOT EXISTS silver.cms_campaign_events (
  id STRING,
	agreement_id INT,
	campaign_id STRING,
	contact_channel_id STRING,
	created_at TIMESTAMP,
	created_by STRING,
	updated_at TIMESTAMP,
	updated_by STRING
)

-- COMMAND ----------

MERGE INTO silver.cms_campaign_events AS target
USING bronze.cms_campaign_events AS source
ON target.id = source.id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
       target.agreement_id       = source.agreement_id       
      ,target.campaign_id        = source.campaign_id        
      ,target.contact_channel_id = source.contact_channel_id 
      ,target.created_at         = source.created_at         
      ,target.created_by         = source.created_by         
      ,target.updated_at         = source.updated_at         
      ,target.updated_by         = source.updated_by         
WHEN NOT MATCHED THEN
  INSERT (
    id
    ,agreement_id
    ,campaign_id
    ,contact_channel_id
    ,created_at
    ,created_by
    ,updated_at
    ,updated_by
  )
  VALUES (
     source.id
    ,source.agreement_id
    ,source.campaign_id
    ,source.contact_channel_id
    ,source.created_at
    ,source.created_by
    ,source.updated_at
    ,source.updated_by
  );
