# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating the Catalog and schemas for the project

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS fraud_detection")
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud_detection.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud_detection.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud_detection.gold")

# COMMAND ----------

#-- Creating Volumn 
spark.sql("""
CREATE VOLUME IF NOT EXISTS fraud_detection.bronze.landing_zone
""") 

# COMMAND ----------

# Source Volumns 
spark.sql("CREATE VOLUME IF NOT EXISTS fraud_detection.bronze.raw_source") 

# COMMAND ----------

# autoloader_schema 
spark.sql("CREATE VOLUME IF NOT EXISTS fraud_detection.bronze.autoloader_metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dividing the ingested csv files into smaller chunks to simulate 

# COMMAND ----------

import pandas as pd
import os

raw_path = "/Volumes/fraud_detection/bronze/raw_source/creditcard.csv"
chunk_dir = "/Volumes/fraud_detection/bronze/raw_source/chunks"
os.makedirs(chunk_dir, exist_ok=True)

df = pd.read_csv(raw_path)
print(f"Total rows: {len(df)}")

chunk_size = 2000  # ~2000 rows per file -> ~143 files
for i, start in enumerate(range(0, len(df), chunk_size)):
    chunk = df.iloc[start:start + chunk_size]
    filename = f"transactions_part{i:04d}.csv"
    chunk.to_csv(os.path.join(chunk_dir, filename), index=False)

print(f"Created {i+1} chunk files in {chunk_dir}")