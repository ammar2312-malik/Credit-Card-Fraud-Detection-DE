# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Transformation Pipeline 

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, sha2, concat_ws

@dlt.table(
    name="fraud_detection.silver.transactions_silver",
    comment="Cleaned and validated credit card transactions"
)
@dlt.expect_or_drop("valid_amount", "Amount >= 0")
@dlt.expect_or_drop("valid_class", "Class IN (0, 1)")
@dlt.expect("no_rescued_data", "_rescued_data IS NULL")
def transactions_silver():
    df = dlt.read_stream("fraud_detection.bronze.transactions")

    return (
        df
        .withColumn("Time", col("Time").cast("double"))
        .withColumn("Amount", col("Amount").cast("double"))
        .withColumn("Class", col("Class").cast("int"))
        .withColumn(
            "transaction_id",
            sha2(concat_ws("_", col("Time"), col("Amount"), col("_source_file")), 256)
        )
        .dropDuplicates(["transaction_id"])
    )

# COMMAND ----------

