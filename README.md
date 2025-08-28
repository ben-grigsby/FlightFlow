# Stock Sentinel

Stock Sentinel is a real-time data pipeline and dashboard that continuously ingests stock market data (e.g., NVDA), stores it in a structured format, applies predictive models, and updates a web dashboard at regular intervals. It is designed to be modular and extendable, with plans to support multi-ticker tracking, automated infrastructure provisioning, and robust orchestration.

---

## Project Objectives

- Stream stock data every few seconds (e.g., via API)
- Perform lightweight transformations and store data efficiently
- Apply and retrain forecasting models on a rolling basis (e.g., every 20 minutes)
- Serve predictions and metrics through a web dashboard
- Automate daily workflow using Airflow
- Extend functionality through Infrastructure as Code (e.g., adding tickers dynamically)

---

## System Architecture

       +-----------------------+
       |   Stock Price API     |
       +----------+------------+
                  |
           [Requests / Kafka]
                  |
       +----------v----------+
       |     ETL + Storage   |
       |  (PostgreSQL / S3)  |
       +----------+----------+
                  |
       +----------v-----------+
       |   Model Training     |
       | (e.g., LinearReg/XGB)|
       +----------+-----------+
                  |
       +----------v-----------+
       |    Web Dashboard     |
       | (Flask / Streamlit)  |
       +----------------------+



---

## Tech Stack

| Component          | Technology               |
|-------------------|--------------------------|
| Data Ingestion     | Python `requests`, Kafka (optional) |
| Orchestration      | Apache Airflow           |
| ETL & Processing   | Pandas, SQL              |
| Storage            | PostgreSQL, optionally S3 |
| Modeling           | scikit-learn, XGBoost    |
| Dashboard          | Streamlit or Flask       |
| Infrastructure     | Terraform / AWS CDK (planned) |

---

## Directory Structure


<pre>
```
stock_sentinel/
├── dags/                    # Airflow DAGs
├── etl/                     # ETL logic
│   ├── fetch_data.py
│   └── transform.py
├── models/                  # Model training and prediction
│   └── regression.py
├── dashboard/               # Flask/Streamlit web app
│   └── app.py
├── infra/                   # IaC scripts (Terraform/CDK)
├── data/                    # Local or cloud-staged data
├── tests/                   # Unit and integration tests
├── requirements.txt
├── README.md
└── main.py                  # Optional script entry point
```
</pre>



---

## Airflow DAG Workflow

| Task                         | Schedule             |
|------------------------------|----------------------|
| Initialize market day        | Daily at 9:30 AM     |
| Stream stock data            | Every 2 seconds      |
| Retrain forecasting model    | Every 20 minutes     |
| Update dashboard             | Every 20 minutes     |
| Close and reset pipeline     | Daily at 4:00 PM     |

---

## Future Enhancements

- Multi-ticker tracking with dynamic input
- Kafka or Kinesis for full real-time stream processing
- IaC interface to deploy pipelines and dashboards for new tickers
- Enhanced dashboards with statistical summaries and model diagnostics
- LLM-based market insights (optional)

---

## Status Tracker

| Component           | Status         |
|--------------------|----------------|
| NVDA Ingestion      | In Progress    |
| Dashboard           | Not Started    |
| Model Integration   | Not Started    |
| Airflow Integration | Planned        |
| IaC / Automation    | Planned        |

---

## Author

**Ben Grigsby**  
Statistics Major, UC Davis  
Focus: Data Engineering & Machine Learning  
