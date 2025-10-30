# HermesFlow

**End-to-End Data Engineering & ML Pipeline for Global Flight Data**

HermesFlow is a full-scale data engineering project that continuously ingests and processes real-world flight data using the AviationStack API.  
It combines stream-style ingestion, batch ETL, Airflow orchestration, and machine learning prediction to forecast future flight delays — all organized within a modern Medallion Architecture (Bronze → Silver → Gold).

---

## Table of Contents

1. [Features](#features)  
2. [Folder Structure](#folder-structure)  
3. [Core Components](#core-components)  
4. [ETL Pipeline (Bronze → Silver → Gold)](#etl-pipeline-bronze--silver--gold)  
5. [Setup](#setup)  
6. [DAG Scheduling Summary](#dag-scheduling-summary)  
7. [Results](#results)  
8. [Future Enhancements](#future-enhancements)  
9. [Author](#author)

---

## Features

- **Dual Pipelines**:  
  - **Historical Flights DAG** — Pulls previous day’s flight data and stores it in an Apache Iceberg table.  
  - **Future Flights DAG** — Fetches scheduled flights 8 days ahead and predicts their expected delays using an XGBoost model.

- **ETL Pipeline**:  
  - Raw JSON → Parquet (Bronze) → Cleaned (Silver) → Iceberg & Predictions (Gold).  

- **ML Integration**:  
  - Weekly retraining on historical data with PySpark + XGBoost.  
  - Daily inference pipeline predicting future arrival delays.  

- **Airflow Orchestration**:  
  - Manages ingestion, transformation, model retraining, and prediction.  

- **Logging & Monitoring**:  
  - Built-in Airflow and Python logging across all tasks.  

- **Scalable Architecture**:  
  - Modularized structure for distributed data engineering and ML processing.

---

## Folder Structure

HermesFlow/
├── dags/                            # Airflow DAG definitions
│   ├── avstack_daily_data_dag.py    # Historical flight data pipeline
│   ├── avstack_future_flight_delay_prediction_dag.py  # Future prediction pipeline
│
├── scripts/                         # Python scripts for ingestion, ML, and utils
│   ├── avstack/
│   │   ├── avstack_kafka_producer.py
│   │   ├── avstack_kafka_consumer.py
│   │   └── utils.py
│   │
│   └── modeling/
│       ├── train_model.py           # Retrains weekly XGBoost model
│       ├── predict_future.py        # Predicts future flight delays
│       └── email_predictions.py     # (Optional) Sends predictions via email
│
├── etl/                             # Modular ETL scripts for each layer
│   ├── future_data/
│   │   ├── future_data_etl.py
│   │   └── future_data_encoding.py
│   └── daily_data/
│       ├── daily_data_etl.py
│       └── daily_data_encoding.py
│
├── data/                            # Output data directory (mounted volume)
│   ├── bronze/                      # Raw JSON from AviationStack
│   ├── silver/                      # Cleaned and structured parquet files
│   ├── gold/                        # Final Iceberg tables and predictions
│   ├── models/                      # Trained ML models
│   └── predictions_excel/           # Excel exports of delay forecasts
│
├── docker-compose.yml               # Airflow, Kafka, Postgres setup
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
└── .env                             # API keys and config variables

---

## Core Components

### 1. Daily Historical Flight Pipeline
- Pulls data from the previous day using the AviationStack API.  
- Converts JSON → Parquet → Iceberg format.  
- Used for analytics, model retraining, and long-term storage.  

### 2. Future Flight Delay Prediction Pipeline
- Fetches flight data 8 days ahead using AviationStack.  
- Applies a trained XGBoost regression model to predict arrival delays.  
- Outputs both Parquet (for reprocessing) and Excel files (for analysis).  

### 3. Weekly Model Retraining
- Historical Iceberg data is aggregated and used to retrain the XGBoost model weekly.  
- Updated model replaces the previous version and is used by the next week’s prediction pipeline.

### 4. Airflow DAG Orchestration
- Manages dependencies across:
  - Data ingestion  
  - Data transformation  
  - Feature encoding  
  - Model prediction  
  - Optional email dispatch  
- Includes automatic retries and logging for fault tolerance.

---

## ETL Pipeline (Bronze → Silver → Gold)

| Layer | Description | Tools |
|-------|--------------|--------|
| **Bronze** | Raw JSON ingestion from AviationStack API. | Python, Requests |
| **Silver** | Data cleaning, schema alignment, and type casting. | PySpark |
| **Gold** | Final Iceberg tables and ML-ready Parquet datasets. | Iceberg, PySpark, Parquet |

---

## Setup

### Prerequisites
- Docker + Docker Compose  
- Python 3.9+  
- [AviationStack API key](https://aviationstack.com/)  

### Quickstart

```
git clone https://github.com/ben-grigsby/HermesFlow.git
cd HermesFlow
cp .env.example .env  # Add your API key
docker-compose up --build
```


Then visit:
	•	Airflow UI: http://localhost:8080

# DAG Scheduling Summary

This document provides an overview of the scheduling logic, execution order, and dependency design of all DAGs within **HermesFlow**.  
Each DAG is orchestrated in **Apache Airflow** to handle data ingestion, transformation, machine learning retraining, and prediction workflows.

---

## Overview

HermesFlow contains three primary DAGs:

1. **Daily Historical Flight Data DAG**
2. **Future Flight Delay Prediction DAG**
3. **Weekly Model Retraining DAG**

These DAGs are designed to complement each other — the **daily pipelines** ensure fresh data ingestion and prediction updates, while the **weekly retraining** ensures model relevance and adaptability over time.

---

## 1. Daily Historical Flight Data DAG (`avstack_daily_data_dag`)

### **Purpose**
Fetches and processes flight data from the previous day (`T-1`) using the AviationStack API.  
The data undergoes a full ETL cycle — from raw JSON ingestion to Iceberg table storage — forming the foundation for retraining and analysis.

### **Schedule**
| Frequency | Time (UTC) | Airflow Expression | Description |
|------------|-------------|--------------------|--------------|
| Daily | 03:00 | `0 3 * * *` | Runs once daily at 3 AM to capture previous day’s flight records. |

### **Workflow**
1. **Data Ingestion** – Requests flight data for `T-1` via AviationStack API.  
2. **Bronze Layer** – Saves raw JSON files.  
3. **Silver Layer** – Converts JSON to Parquet, performs schema standardization.  
4. **Gold Layer** – Stores processed data into Apache Iceberg tables.  
5. **Logging** – Each step tracked within Airflow for transparency and error recovery.

### **Output**
- `data/bronze/daily/` → Raw data  
- `data/silver/daily/` → Cleaned parquet  
- `data/gold/iceberg/` → Final storage for analytics and ML training  

---

## 2. Future Flight Delay Prediction DAG (`avstack_future_flight_delay_prediction`)

### **Purpose**
Pulls future scheduled flights (`T+8`) and predicts their expected delays using a pre-trained **XGBoost regression model**.  
This DAG ensures the system continuously outputs actionable insights for upcoming flights.

### **Schedule**
| Frequency | Time (UTC) | Airflow Expression | Description |
|------------|-------------|--------------------|--------------|
| Daily | 10:00 | `0 10 * * *` | Runs once daily at 10 AM to forecast flight delays 8 days ahead. |

### **Workflow**
1. **Data Ingestion** – Fetches upcoming flight data via AviationStack API.  
2. **ETL & Encoding** – Converts to Parquet and prepares model-ready features.  
3. **Model Prediction** – Uses latest trained XGBoost model for delay forecasting.  
4. **Output Generation** – Saves both Parquet and Excel outputs for reporting.  
5. **(Optional)** Email Dispatch – Sends delay reports to subscribed users.

### **Output**
- `data/predictions/future_flights/` → Raw predictions in Parquet  
- `data/predictions_excel/` → Readable Excel summaries  

---

## 3. Weekly Model Retraining DAG (`train_xgboost_model`)

### **Purpose**
Retrains the **XGBoost regression model** using the latest historical flight data stored in Iceberg format.  
This ensures continuous improvement of prediction accuracy as new data becomes available.

### **Schedule**
| Frequency | Time (UTC) | Airflow Expression | Description |
|------------|-------------|--------------------|--------------|
| Weekly | Sunday, 04:00 | `0 4 * * 0` | Runs once weekly at 4 AM every Sunday. |

### **Workflow**
1. **Data Extraction** – Pulls aggregated flight records from Iceberg.  
2. **Feature Engineering** – Generates lag, rolling, and encoded variables.  
3. **Model Training** – Trains a new XGBoost model using PySpark ML interface.  
4. **Validation** – Evaluates model performance (MAE, RMSE).  
5. **Deployment** – Replaces existing model in `/data/models/xgboost_flight_delay/`.

### **Output**
- `data/models/xgboost_flight_delay/` → Serialized SparkXGBRegressor model  
- `data/logs/model_training.log` → Metrics and versioning info  

---

## Dependency Flow

The following diagram illustrates the dependency order between DAGs:


```text
          ┌────────────────────────────┐
          │   avstack_daily_data_dag   │   (Runs daily at 3 AM UTC)
          └────────────┬───────────────┘
                       │
                       ▼
          ┌────────────────────────────┐
          │   train_xgboost_model      │   (Runs weekly on Sunday at 4 AM UTC)
          └────────────┬───────────────┘
                       │
                       ▼
          ┌──────────────────────────────────────────────┐
          │ avstack_future_flight_delay_prediction       │   (Runs daily at 10 AM UTC)
          └──────────────────────────────────────────────┘


**Logic:**  
1. The **daily DAG** ensures historical data is fresh and ready for retraining.  
2. The **weekly retrain DAG** updates the model based on that data.  
3. The **future prediction DAG** consumes the latest model and predicts upcoming delays.

---

## Notes on Reliability

- All DAGs are configured with:
  - **`catchup=False`** to avoid retroactive backfills  
  - **Logging and XComs** for consistent inter-task data passing  

- The pipelines gracefully handle:
  - Temporary API outages  
  - Slow or malformed responses  
  - Partial data writes  

---

## Summary

| DAG Name | Trigger Time | Frequency | Data Target | Core Output |
|-----------|--------------|------------|--------------|--------------|
| `avstack_daily_data_dag` | 03:00 UTC | Daily | Iceberg | Historical flight data |
| `train_xgboost_model` | 04:00 UTC (Sunday) | Weekly | Models directory | Updated XGBoost model |
| `avstack_future_flight_delay_prediction` | 10:00 UTC | Daily | Parquet + Excel | Future flight delay forecasts |

---

## Author

**Ben Grigsby**  
Statistics & Machine Learning @ UC Davis  
[GitHub](https://github.com/ben-grigsby) • [LinkedIn](https://linkedin.com/in/ben-grigsby)