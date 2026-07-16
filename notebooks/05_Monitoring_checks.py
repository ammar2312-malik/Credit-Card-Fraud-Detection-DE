# Databricks notebook source
# MAGIC %md
# MAGIC ## Monitoring Checks 

# COMMAND ----------

for table in ["bronze.transactions", "silver.transactions_silver", "gold.transactions_gold", "gold.scored_transactions"]:
    cnt = spark.sql(f"SELECT count(*) as cnt FROM fraud_detection.{table}").collect()[0]["cnt"]
    print(f"{table}: {cnt} rows")

# Data quality — rescued data in bronze
rescued = spark.sql("""
    SELECT count(*) as cnt FROM fraud_detection.bronze.transactions
    WHERE _rescued_data IS NOT NULL
""").collect()[0]["cnt"]
print(f"Rescued/malformed rows in bronze: {rescued}")

# Freshness — when did data last land
latest_ingest = spark.sql("""
    SELECT max(_ingest_timestamp) as latest FROM fraud_detection.bronze.transactions
""").collect()[0]["latest"]
print(f"Last ingestion timestamp: {latest_ingest}")

# Fraud rate trend (scored data)
spark.sql("""
    SELECT predicted_fraud, count(*) as cnt, avg(fraud_probability) as avg_prob
    FROM fraud_detection.gold.scored_transactions
    GROUP BY predicted_fraud
""").show()