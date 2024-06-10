# Databricks notebook source
tables = ['product_layer_product']
for table in tables: 
    df = spark.read.table(f"bronze.{table}")
    df.write.mode("overwrite").saveAsTable(f"silver.{table}")
