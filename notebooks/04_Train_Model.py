# Databricks notebook source
# MAGIC %md
# MAGIC ## Train The Model MLFLow 

# COMMAND ----------

# Databricks notebook: train_model
# Step 6 - Train fraud detection model on Gold features, log to MLflow, register in Unity Catalog

# ---------------------------------------------------------------------------
# 1. Imports & MLflow setup
# ---------------------------------------------------------------------------
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    precision_score,
    recall_score,
    average_precision_score,
    classification_report,
    confusion_matrix,
)

# Make sure MLflow registers models into Unity Catalog (not the legacy workspace registry)
mlflow.set_registry_uri("fraud_detection")

EXPERIMENT_NAME = "/Shared/fraud_detection_experiment"
REGISTERED_MODEL_NAME = "fraud_detection.ml.fraud_classifier"

mlflow.set_experiment(EXPERIMENT_NAME)

# ---------------------------------------------------------------------------
# 2. Load Gold features and prep train/test split
# ---------------------------------------------------------------------------
df = spark.table("fraud_detection.gold.transactions_gold").toPandas()

# Drop rows with nulls in engineered features (first-txn-per-card edge cases)
df = df.dropna(
    subset=["time_since_last_txn", "avg_amount_last_10_txns", "amount_zscore_per_card"]
)

feature_cols = [c for c in df.columns if c.startswith("V")] + [
    "Amount",
    "time_since_last_txn",
    "avg_amount_last_10_txns",
    "stddev_amount_last_10_txns",
    "amount_zscore_per_card",
    "txn_count_last_1hr",
]

X = df[feature_cols]
y = df["Class"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Train: {len(X_train)}, Test: {len(X_test)}, Fraud rate in train: {y_train.mean():.4f}")

# ---------------------------------------------------------------------------
# 3. Train with MLflow autologging + explicit metrics
# ---------------------------------------------------------------------------
mlflow.sklearn.autolog()

with mlflow.start_run(run_name="random_forest_baseline") as run:
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=10,
        class_weight="balanced",  # important given fraud class imbalance
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    auprc = average_precision_score(y_test, y_proba)
    cm = confusion_matrix(y_test, y_pred)

    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("auprc", auprc)

    print(classification_report(y_test, y_pred))
    print(f"AUPRC: {auprc:.4f}")
    print(f"Confusion matrix [[TN, FP],[FN, TP]]:\n{cm}")

    run_id = run.info.run_id
    print(f"Run ID: {run_id}")

    # -----------------------------------------------------------------
    # 4. Register the model in Unity Catalog (inside the same run)
    # -----------------------------------------------------------------
    mlflow.sklearn.log_model(
    sk_model=model,
    artifact_path="model",
    registered_model_name=REGISTERED_MODEL_NAME,
    input_example=X_train.iloc[:5],   
)

print(f"Model registered as: {REGISTERED_MODEL_NAME}")

# ---------------------------------------------------------------------------
# 5. Verify registration
# ---------------------------------------------------------------------------
from mlflow import MlflowClient

client = MlflowClient()
latest_versions = client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")

for v in latest_versions:
    print(f"Version: {v.version}, Run ID: {v.run_id}, Status: {v.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Scoring - GOLD 

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("fraud_detection")

MODEL_NAME = "fraud_detection.ml.fraud_classifier"

from mlflow import MlflowClient
client = MlflowClient()

# get the latest version number dynamically instead of hardcoding
latest_version = max(
    int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'")
)
print(f"Loading version: {latest_version}")

model_uri = f"models:/{MODEL_NAME}/{latest_version}"
loaded_model = mlflow.sklearn.load_model(model_uri)

# COMMAND ----------

import pandas as pd 
feature_cols = [c for c in spark.table("fraud_detection.gold.transactions_gold").columns if c.startswith("V")] + [
    "Amount", "time_since_last_txn", "avg_amount_last_10_txns",
    "stddev_amount_last_10_txns", "amount_zscore_per_card", "txn_count_last_1hr"
]

score_df = spark.table("fraud_detection.gold.transactions_gold").toPandas()
score_df = score_df.dropna(subset=["time_since_last_txn", "avg_amount_last_10_txns", "amount_zscore_per_card"])

X_score = score_df[feature_cols]

score_df["predicted_fraud"] = loaded_model.predict(X_score)
score_df["fraud_probability"] = loaded_model.predict_proba(X_score)[:, 1]
score_df["model_version"] = latest_version
score_df["scored_at"] = pd.Timestamp.utcnow()

# COMMAND ----------

scored_spark_df = spark.createDataFrame(score_df)

scored_spark_df.write.format("delta").mode("overwrite").saveAsTable(
    "fraud_detection.gold.scored_transactions"
)

# COMMAND ----------

spark.sql("""
SELECT predicted_fraud, count(*) as cnt, avg(fraud_probability) as avg_prob
FROM fraud_detection.gold.scored_transactions
GROUP BY predicted_fraud
""").show()

# COMMAND ----------

spark.sql("""
SELECT Class, predicted_fraud, fraud_probability 
FROM fraud_detection.gold.scored_transactions 
WHERE Class = 1
ORDER BY fraud_probability DESC
""").show(20)