# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader file we will ingest the file using it into the bronze delta table  

# COMMAND ----------

# COnfigure the paths 
landing_zone    = "/Volumes/fraud_detection/bronze/landing_zone"
schema_path     = "/Volumes/fraud_detection/bronze/autoloader_metadata/schema"
checkpoint_path = "/Volumes/fraud_detection/bronze/autoloader_metadata/checkpoint"

# COMMAND ----------

# 2:- Read the stream with autoloader 
from pyspark.sql.functions import * 

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(landing_zone)
)

df_bronze = (
    df
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

query = (
    df_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("fraud_detection.bronze.transactions")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verifying the Gold Analytics 
# MAGIC

# COMMAND ----------

display(spark.table("fraud_detection.gold.transactions_gold"))