# 🛡️ Credit Card Fraud Detection — End-to-End Data Engineering Platform

<p align="center">
  <img src="https://img.shields.io/badge/Platform-Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white" />
  <img src="https://img.shields.io/badge/Language-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/ML-MLflow-0194E2?style=for-the-badge&logo=mlflow&logoColor=white" />
  <img src="https://img.shields.io/badge/Status-Complete-2ECC71?style=for-the-badge" />
</p>

<p align="center">
  <b>A production-style, fully orchestrated fraud detection platform</b> — built end-to-end on the Databricks Lakehouse, using Autoloader, Delta Lake, Lakeflow Declarative Pipelines, Unity Catalog, and MLflow.
</p>

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Data Pipeline Walkthrough](#-data-pipeline-walkthrough)
- [Machine Learning](#-machine-learning)
- [Orchestration](#-orchestration)
- [Monitoring Dashboard](#-monitoring-dashboard)
- [Results](#-results)
- [Known Limitations & Design Tradeoffs](#-known-limitations--design-tradeoffs)
- [Future Enhancements](#-future-enhancements)
- [How to Run](#-how-to-run)
- [Author](#-author)

---

## 🔍 Overview

Fraud detection is a deceptively hard problem: fraud is rare (<1% of transactions), highly behavioral, and time-dependent — meaning a good solution needs more than a model, it needs a **platform**: reliable ingestion, enforced data quality, stateful feature engineering, governed ML, and operational visibility.

This project builds that full platform on **Databricks Free Edition**, simulating a real-world bank/payments data team's stack:

> 📥 Files land in storage → 🔄 Autoloader ingests incrementally → 🧱 Medallion architecture cleans & enriches → 🧠 ML model scores fraud risk → 📊 Dashboard shows it live — all triggered by one scheduled Workflow.

---

## 🏗️ Architecture

```
                    ┌───────────────────────────┐
   Simulated feed → │ Landing Zone (UC Volume)  │  raw CSV files arriving incrementally
                    └────────────┬──────────────┘
                                 │   🔄 Autoloader (cloudFiles)
                                 ▼
                    ┌───────────────────────────┐
                    │   🥉 BRONZE (Delta)       │  raw + ingestion metadata + rescued data
                    └────────────┬──────────────┘
                                 │   🧹 Lakeflow Declarative Pipeline + expectations
                                 ▼
                    ┌───────────────────────────┐
                    │   🥈 SILVER (Delta)       │  validated, deduplicated transactions
                    └────────────┬──────────────┘
                                 │   ⚙️ Feature engineering (Spark window functions)
                                 ▼
                    ┌───────────────────────────┐
                    │   🥇 GOLD (Delta)         │  ML-ready behavioral features
                    └────────────┬──────────────┘
                                 │
                     ┌───────────┴────────────┐
                     ▼                        ▼
              🧠 MLflow Model          📊 Batch Scoring
        (train → register → version)   → scored_transactions
                     │                        │
                     └───────────┬────────────┘
                                 ▼
                    ┌───────────────────────────┐
                    │  📈 AI/BI Dashboard       │  fraud trends, model health, freshness
                    └───────────────────────────┘

        🔗 All stages orchestrated end-to-end via Databricks Workflows
```

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| ☁️ Compute Platform | Databricks Free Edition (Serverless) |
| 🗄️ Storage Format | Delta Lake |
| 📥 Ingestion | Databricks Autoloader (`cloudFiles`) |
| 🧹 Data Quality / ETL | Lakeflow Declarative Pipelines (formerly Delta Live Tables) |
| 🔐 Governance | Unity Catalog (Catalogs, Schemas, Volumes, Model Registry) |
| 🧠 ML Tracking & Registry | MLflow |
| 🤖 Modeling | scikit-learn (Random Forest, class-weighted) |
| ⚡ Processing Engine | Apache Spark (PySpark) |
| 🔗 Orchestration | Databricks Workflows |
| 📊 Monitoring / BI | Databricks AI/BI Dashboards |

---

## 📁 Project Structure

```
fraud-detection-capstone/
├── README.md
├── requirements.txt
│
├── data/
│   ├── raw_source/                     # original Kaggle dataset
│   └── simulated_feed/                 # chunked files for the drip-feed simulation
│
├── src/
│   ├── ingestion/
│   │   ├── autoloader_bronze.py        # Autoloader: landing zone → bronze
│   │   └── simulate_feed.py            # drips files into the landing zone
│   │
│   ├── pipelines/                      # Lakeflow Declarative Pipeline definitions
│   │   ├── silver_transactions.py      # cleaning + expectations
│   │   └── gold_features.py            # rolling behavioral features
│   │
│   ├── ml/
│   │   ├── train_model.py              # MLflow training + Unity Catalog registration
│   │   └── batch_score.py              # loads latest model, scores gold table
│   │
│   └── monitoring/
│       └── monitoring_checks.py        # row counts, freshness, rescued-data checks
│
├── workflows/
│   └── fraud_pipeline_job.yml          # orchestration DAG definition
│
├── dashboards/
│   └── fraud_overview_dashboard.json   # exported AI/BI dashboard
│
└── docs/
    └── PRD.docx
```

---

## 🔄 Data Pipeline Walkthrough

### 🥉 Bronze — Incremental Ingestion
- **Autoloader** (`cloudFiles`) watches a Unity Catalog Volume and ingests new files incrementally and idempotently.
- Schema evolution is handled automatically via a tracked schema location.
- Malformed/schema-drifted records are captured in a `_rescued_data` column instead of failing the pipeline.
- Verified: re-running ingestion after new files land only processes the *new* rows — no duplication.

### 🥈 Silver — Data Quality Enforcement
- Built as a **Lakeflow Declarative Pipeline** with declarative `@dlt.expect` constraints.
- Enforces valid amount ranges, valid class labels, and deduplicates on a derived transaction key.
- Produces a built-in data-quality panel showing pass/fail rates per rule.

### 🥇 Gold — Feature Engineering
Using Spark **window functions**, the pipeline computes genuine behavioral fraud signals:

| Feature | Signal |
|---|---|
| `amount_zscore_per_card` | Unusual spend vs. that card's own history |
| `time_since_last_txn` | Rapid-fire "card testing" patterns |
| `avg_amount_last_10_txns` | Rolling spend baseline |
| `txn_count_last_1hr` | Transaction bursts in a trailing time window |

---

## 🧠 Machine Learning

- **Model:** Random Forest, `class_weight="balanced"` to handle severe class imbalance (<1% fraud rate).
- **Tracking:** every run logged via `mlflow.autolog()` — params, metrics, and model artifacts.
- **Evaluation:** Precision, Recall, and **AUPRC** (the correct metric for imbalanced classification — not accuracy).
- **Registry:** registered in the **Unity Catalog Model Registry** with a full input/output signature for governed versioning.
- **Scoring:** batch job dynamically loads the *latest* registered version — no hardcoded version numbers.

---

## 🔗 Orchestration

All stages are chained into a single **Databricks Workflow** with explicit task dependencies:

```
bronze_ingestion  →  silver_gold_pipeline  →  train_and_score  →  dashboard_refresh
```

- Runs on a schedule (triggered/batch mode — quota-friendly for serverless compute).
- Each task's success/failure is visible in the Workflow's DAG view.
- Failure notifications configured on job failure.

---

## 📊 Monitoring Dashboard

A published, auto-refreshing **AI/BI Dashboard** — *"Credit Card Fraud Detection — Operations Dashboard"* — provides live visibility:

| Tile | What it shows |
|---|---|
| 📈 Fraud Detection Rate Over Time | Trend of flagged transactions as a % of volume |
| 📊 Transaction Volume vs. Flagged Transactions | Total vs. fraud-flagged volume over time |
| 🎯 Model Health | True positives, false negatives, precision, recall |
| 🧱 Data Pipeline Volume (Bronze → Silver → Gold) | Row counts per layer, visualizing data flow |
| 🟢 Pipeline Freshness | Minutes since last ingestion, color-coded health status |

---

## 📈 Results

| Metric | Value |
|---|---|
| **Precision** | 1.00 |
| **Recall** | 0.786 |
| **AUPRC** | 0.929 |
| **False Positives** | 0 |
| **False Negatives** | 3 (of 14 fraud cases) |

> ✅ The model catches the large majority of fraud with **zero false positives** — a conservative, customer-friendly starting point with a clear path to recall improvement via threshold tuning or ensemble methods.

---

## ⚠️ Known Limitations & Design Tradeoffs

Being upfront about tradeoffs here — these are deliberate, documented decisions, not oversights:

- 🔑 **No true card ID in the source dataset** — a derived `pseudo_card_id` proxy is used for grouping/deduplication, which may inflate feature usefulness slightly vs. a real production dataset.
- 🔁 **Full-table batch rescoring** is used instead of incremental `MERGE`-based scoring, for simplicity at this scale.
- 🎯 **Model retraining is manual**, not yet triggered automatically by drift detection.
- 🧮 **Silver-layer deduplication** dropped ~3% of Bronze rows — root-caused and confirmed as expected behavior from the synthetic transaction key, not silent data loss.

---

## 🚀 Future Enhancements

- [ ] Incremental `MERGE`-based scoring instead of full-table rescoring
- [ ] Automated model retraining on drift detection
- [ ] Real-time streaming feature computation for lower-latency scoring
- [ ] Unity Catalog Feature Store instead of a plain Gold feature table
- [ ] SCD Type 2 modeling for card/merchant dimension tables

---

## ▶️ How to Run

1. Create a **Databricks Free Edition** workspace.
2. Set up Unity Catalog structure: `fraud_detection` catalog with `bronze`, `silver`, `gold`, `ml` schemas and volumes (see `src/ingestion/`).
3. Download the [Kaggle Credit Card Fraud dataset](https://www.kaggle.com/mlg-ulb/creditcardfraud) into the `raw_source` volume.
4. Run `simulate_feed.py` to chunk and drip files into the landing zone.
5. Run `autoloader_bronze.py` to populate the Bronze table.
6. Deploy `silver_transactions.py` + `gold_features.py` as a Lakeflow Declarative Pipeline.
7. Run `train_model.py` to train, log, and register the model in MLflow / Unity Catalog.
8. Run `batch_score.py` to generate `scored_transactions`.
9. Wire everything into a Databricks Workflow (`workflows/fraud_pipeline_job.yml`) and set a schedule.
10. Import `dashboards/fraud_overview_dashboard.json` and publish.

---

## 👤 Author Ammar Malik 
Built as a hands-on Data Engineering + ML capstone, demonstrating incremental ingestion, medallion architecture, declarative data quality, governed ML lifecycle management, and full pipeline orchestration on the Databricks Lakehouse.

<p align="center">⭐ If you found this project useful or interesting, consider starring the repo!</p>
