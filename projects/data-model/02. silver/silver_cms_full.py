# Databricks notebook source
tables = ['cms_company', 'cms_country']
for table in tables: 
    df = spark.read.table(f"bronze.{table}")
    df.write.mode("overwrite").saveAsTable(f"silver.{table}")
