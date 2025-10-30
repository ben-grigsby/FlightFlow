# HermesFlow

**End-to-End Data Engineering & ML Pipeline for Global Flight Data**

---

## Purpose

HermesFlow is designed to help **airlines and airports** organize and analyze global flight information efficiently.  
In particular, it focuses on **JFK Airport**, where it predicts **arrival delays** for incoming flights up to **eight days in advance**.  
Flight data is retrieved from the **AviationStack API**, which allows access to schedules up to **eight days in the future** — the earliest window available for forecasting.  

This capability is critical for **resource planning, gate allocation, and passenger communication**, making the system not just a technical showcase but a **practical decision-support tool** for aviation operations.

---

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
7. [Future Enhancements](#future-enhancements)  
8. [Project Reflection](#project-reflection)
9. [Author](#author)

---

## Features

- **Kafka-Based Data Ingestion**:  
  - AviationStack flight data (historical and future) is streamed into Kafka topics.  
  - Kafka acts as a resilient buffer, decoupling data ingestion from processing.  

- **Dual Pipelines**:  
  - **Historical Flights DAG** — Pulls previous day’s flight data and stores it in an Apache Iceberg table.  
  - **Future Flights DAG** — Fetches scheduled flights 8 days ahead and predicts their expected delays using an XGBoost model.

- **ETL Pipeline**:  
  - Raw JSON (Bronze) → Cleaned Parquet (Silver) → Iceberg & Predictions (Gold).  

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

```
HermesFlow/
├── dags/                            # Airflow DAG definitions
│   ├── avstack_daily_data_dag.py    # Historical flight data pipeline
│   ├── avstack_future_flight_delay_prediction_dag.py  # Future prediction pipeline
│
├── dashboard/                       # Dashobard definitions
│   ├── load_data.py                 # Loads in data from iceberg warehouse
│   ├── main.py                      # Full code and structure for the dashboard
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
├── PROJECT_REFLECTION.md            # Deep reflection about tool choices, motivations, and other insights
└── .env                             # API keys and config variables
```

---

## Core Components

### 1. Kafka Streaming Backbone
- Kafka handles **real-time ingestion and buffering** of AviationStack data.  
- The **producer** (`avstack_kafka_producer.py`) fetches API data and publishes JSON payloads into a Kafka topic which begins the **Bronze layer**.  
- The **consumer** (`avstack_kafka_consumer.py`) reads from that topic, writing structured messages to end the **Bronze layer**.  
- This setup provides durability, retry handling, and message-level fault tolerance.


### 2. Daily Historical Flight Pipeline
- Pulls data from the previous day using the AviationStack API.  
- Converts JSON → Parquet → Iceberg format.  
- Used for analytics, model retraining, and long-term storage.  

### 3. Future Flight Delay Prediction Pipeline
- Fetches flight data **up to 8 days in advance** using the AviationStack API - the **earliest avialable future window** supported by their service.
- Applies a trained XGBoost regression model to predict arrival delays.  
- Outputs both Parquet (for reprocessing) and Excel files (for analysis).  

### 4. Weekly Model Retraining
- Historical Iceberg data is aggregated and used to retrain the XGBoost model weekly.  
- Updated model replaces the previous version and is used by the next week’s prediction pipeline.

### 5. Airflow DAG Orchestration
- Manages dependencies across:
  - Data ingestion  
  - Data transformation  
  - Feature encoding  
  - Model prediction  
  - **Optional** email dispatch  
- Includes automatic retries and logging for fault tolerance.

---

## ETL Pipeline (Bronze → Silver → Gold)

| Layer | Description | Tools |
|-------|--------------|--------|
| **Bronze** | Raw JSON ingestion from AviationStack API. | Python, Kafka |
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

- Airflow UI: http://localhost:8080

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

Consumes and processes flight data from the previous day (T-1) via Kafka consumer streams.
Performs the complete ETL cycle and stores the output in Iceberg for retraining and analytics.

### **Schedule**
| Frequency | Time (UTC) | Airflow Expression | Description |
|------------|-------------|--------------------|--------------|
| Daily | 03:00 | `0 3 * * *` | Runs once daily at 3 AM (UTC) to capture previous day’s flight records. |

### **Workflow**

1. **Bronze Layer** – Streams AviationStack flight data into **daily_flights** and reads messages while converting them to raw JSON.  
2. **Silver Layer** – Converts JSON to Parquet, performs schema standardization.  
3. **Gold Layer** – Stores processed data into Apache Iceberg tables.  
4. **Logging** – Each step tracked within Airflow for transparency and error recovery.

### **Output**
- `data/bronze/daily/` → Kafka streaming and data processing
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
1. **Kafka Producer** – Publishes T+8 flight data to future_flights topic.
2. **Kafka Consumer** – Writes records to Bronze parquet.  
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


```
          ┌────────────────────────────┐
          │   avstack_daily_data_dag   │   (Runs daily at 3 AM UTC)
          └────────────┬───────────────┘
                       │
                       ▼
          ┌────────────────────────────┐
          │   train_xgboost_model      │   (Runs weekly on Sunday at 5 AM UTC)
          └────────────┬───────────────┘
                       │
                       ▼
          ┌──────────────────────────────────────────────┐
          │ avstack_future_flight_delay_prediction       │   (Runs daily at 2 AM UTC)
          └──────────────────────────────────────────────┘
```

**Logic:**  
1. The **daily DAG** ensures historical data is fresh and ready for retraining.  
2. The **weekly retrain DAG** updates the model based on that data.  
3. The **future prediction DAG** consumes the latest model and predicts upcoming delays.

---

## Notes on Reliability

- All DAGs are configured with:
  - **`catchup=False`** to avoid retroactive backfills  
  - **Logging and XComs** for consistent inter-task data passing  
  - **Kafka offset tracking** for reliable message consumption
  - **Retries** for transient API or network errors

- The pipelines gracefully handle:
  - Temporary API outages  
  - Slow or malformed responses  
  - Partial data writes  
  - Kafka retries

---

## Summary

| DAG Name | Trigger Time | Frequency | Data Target | Core Output |
|-----------|--------------|------------|--------------|--------------|
| `avstack_daily_data_dag` | 03:00 UTC | Daily | Iceberg | Historical flight data |
| `train_xgboost_model` | 04:00 UTC (Sunday) | Weekly | Models directory | Updated XGBoost model |
| `avstack_future_flight_delay_prediction` | 10:00 UTC | Daily | Parquet + Excel | Future flight delay forecasts |

---

## Future Enhancements

1. Adding in a pipeline that extracts weather data to become an additional model variable and further improve delay prediction accuracy 
2. Update the delay prediction output code to email analysts/schedule planners the prediction status of flights to aid their efforts in mitigating costs caused by delays.

---

## Project Reflection

A deeper analysis of design decisions, trade-offs, and lessons learned can be found in the  
📘 [DEEP_REFLECTION.md](./PROJECT_REFLECTION.md) document.

---

## Author

**Ben Grigsby**  
Statistics & Machine Learning @ UC Davis  
[GitHub](https://github.com/ben-grigsby) • [LinkedIn](https://linkedin.com/in/ben-grigsby)