# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="fraud_detection.gold.transactions_gold",
    comment="ML-ready feature table for fraud scoring"
)
def transactions_gold():
    df = dlt.read("fraud_detection.silver.transactions_silver")

    # Since this dataset has no real card ID, we bucket by a stable proxy
    # (V1 rounded) to simulate grouping transactions from "the same card".
    # NOTE: this is a stand-in -- swap for a real card_id in a production dataset.
    df = df.withColumn("pseudo_card_id", F.round(F.col("V1"), 0).cast("int"))

    card_window = Window.partitionBy("pseudo_card_id").orderBy("Time")
    rolling_10 = card_window.rowsBetween(-10, -1)

    df = (
        df
        .withColumn("prev_txn_time", F.lag("Time").over(card_window))
        .withColumn("time_since_last_txn",
                    F.when(F.col("prev_txn_time").isNotNull(),
                           F.col("Time") - F.col("prev_txn_time")))
        .withColumn("avg_amount_last_10_txns", F.avg("Amount").over(rolling_10))
        .withColumn("stddev_amount_last_10_txns", F.stddev("Amount").over(rolling_10))
        .withColumn(
            "amount_zscore_per_card",
            F.when(
                F.col("stddev_amount_last_10_txns") > 0,
                (F.col("Amount") - F.col("avg_amount_last_10_txns")) / F.col("stddev_amount_last_10_txns")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "txn_count_last_1hr",
            F.count("Time").over(card_window.rangeBetween(-3600, -1))
        )
        .drop("prev_txn_time")
    )

    return df