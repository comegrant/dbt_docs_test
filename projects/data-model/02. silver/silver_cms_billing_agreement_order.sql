-- Databricks notebook source
CREATE TABLE IF NOT EXISTS silver.cms_billing_agreement_order (
  id STRING,
  agreement_id INT,
  week INT,
  year INT,
  shipping_address_id STRING,
  payment_method STRING,
  totalAmount FLOAT,
  order_id BIGINT,
  delivery_date DATE,
  transaction_id STRING,
  transactionNumber INT,
  transaction_date TIMESTAMP,
  created_date TIMESTAMP,
  invoice_status INT,
  invoice_status_date DATE,
  sentToInvoicePartner BOOLEAN,
  sumOfOrderLinesPrice FLOAT,
  userType STRING,
  invoice_data_create TIMESTAMP,
  billing_address_id STRING,
  version_ShippingAddress INT,
  version_BillingAddress INT,
  order_type STRING,
  payment_partner_id STRING,
  invoice_processed INT,
  order_campaign_id STRING,
  order_status_id STRING,
  invoice_processed_at TIMESTAMP,
  timeblock INT,
  original_timeblock INT,
  cutoff_date TIMESTAMP,
  has_recipe_leaflets BOOLEAN
)

-- COMMAND ----------

MERGE INTO silver.cms_billing_agreement_order AS target
USING bronze.cms_billing_agreement_order AS source
ON target.id = source.id
WHEN MATCHED AND source.created_date > target.created_date THEN
  UPDATE SET
   target.agreement_id = source.agreement_id
  , target.week = source.week
  , target.year = source.year
  , target.shipping_address_id = source.shipping_address_id
  , target.payment_method = source.payment_method
  , target.totalAmount = source.totalAmount
  , target.order_id = source.order_id
  , target.delivery_date = source.delivery_date
  , target.transaction_id = source.transaction_id
  , target.transactionNumber = source.transactionNumber
  , target.transaction_date = source.transaction_date
  , target.created_date = source.created_date
  , target.invoice_status = source.invoice_status
  , target.invoice_status_date = source.invoice_status_date
  , target.sentToInvoicePartner = source.sentToInvoicePartner
  , target.sumOfOrderLinesPrice = source.sumOfOrderLinesPrice
  , target.userType = source.userType
  , target.invoice_data_create = source.invoice_data_create
  , target.billing_address_id = source.billing_address_id
  , target.version_ShippingAddress = source.version_ShippingAddress
  , target.version_BillingAddress = source.version_BillingAddress
  , target.order_type = source.order_type
  , target.payment_partner_id = source.payment_partner_id
  , target.invoice_processed = source.invoice_processed
  , target.order_campaign_id = source.order_campaign_id
  , target.order_status_id = source.order_status_id
  , target.invoice_processed_at = source.invoice_processed_at
  , target.timeblock = source.timeblock
  , target.original_timeblock = source.original_timeblock
  , target.cutoff_date = source.cutoff_date
  , target.has_recipe_leaflets = source.has_recipe_leaflets
WHEN NOT MATCHED THEN
  INSERT (
    id 
  , agreement_id
  , week
  , year
  , shipping_address_id
  , payment_method
  , totalAmount
  , order_id
  , delivery_date
  , transaction_id
  , transactionNumber
  , transaction_date
  , created_date
  , invoice_status
  , invoice_status_date
  , sentToInvoicePartner
  , sumOfOrderLinesPrice
  , userType
  , invoice_data_create
  , billing_address_id
  , version_ShippingAddress
  , version_BillingAddress
  , order_type
  , payment_partner_id
  , invoice_processed
  , order_campaign_id
  , order_status_id
  , invoice_processed_at
  , timeblock
  , original_timeblock
  , cutoff_date
  , has_recipe_leaflets
  )
  VALUES (
   source.id 
  ,source.agreement_id
  , source.week
  , source.year
  , source.shipping_address_id
  , source.payment_method
  , source.totalAmount
  , source.order_id
  , source.delivery_date
  , source.transaction_id
  , source.transactionNumber
  , source.transaction_date
  , source.created_date
  , source.invoice_status
  , source.invoice_status_date
  , source.sentToInvoicePartner
  , source.sumOfOrderLinesPrice
  , source.userType
  , source.invoice_data_create
  , source.billing_address_id
  , source.version_ShippingAddress
  , source.version_BillingAddress
  , source.order_type
  , source.payment_partner_id
  , source.invoice_processed
  , source.order_campaign_id
  , source.order_status_id
  , source.invoice_processed_at
  , source.timeblock
  , source.original_timeblock
  , source.cutoff_date
  , source.has_recipe_leaflets
  );
