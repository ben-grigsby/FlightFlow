# FlightFlow

**Real-Time and Batch ETL Pipeline for Global Flight Data**

FlightFlow is an end-to-end data engineering project that streams and processes real-world flight data using the OpenSky Network and AviationStack APIs. It incorporates streaming pipelines with Kafka, batch ingestion, DAG orchestration via Airflow, multi-layered data storage, and alerting on flight altitude violations — all while showcasing a production-grade data architecture.

---

## Features

- **Real-Time Streaming**: Pulls flight data from OpenSky every 11 seconds, stores it in Kafka.
- **Batch ETL**: Ingests AviationStack API data every 4 hours and processes it through the full ETL pipeline.
- **ETL Pipeline**: Data flows through Bronze → Silver → Gold layers.
- **Kafka Consumer DAG**: Periodically pulls from Kafka to feed the pipeline (every 5 minutes).
- **Airflow DAGs**: Handles orchestration, scheduling, and logging.
- **Anomaly Detection**: Alerts when aircraft dip below a specified altitude.
- **Logging & Monitoring**: Integrated logging for each DAG and streaming service.
- **Scalable Structure**: Clean, modular folder design based on medallion architecture.

---

## Folder Structure

```
FlightFlow/
├── dags/                            # Airflow DAG definitions
│   ├── aviationstack_etl_dag.py
│   ├── opensky_data_streaming_dag.py
│   ├── opensky_kafka_consumer_dag.py
│   └── opensky_altitude_alert_dag.py
│
├── scripts/                         # Python scripts for Kafka and preprocessing
│   └── opensky/
│       ├── bash_streaming.py       # Starts and manages the Kafka producer loop
│       ├── kafka_producer.py
│       ├── kafka_consumer.py
│       ├── alert_checker.py        # Altitude-based anomaly detection
│       └── transform_utils.py      # Cleans and enriches raw flight data
│
├── data/                            # Output data and Kafka logs
│   └── kafka_logs/
│       └── opensky/
│           └── as_message_*.json
│
├── etl/                             # Modular ETL layer scripts
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   ├── gold_layer.py
│   └── utils.py
│
├── docker-compose.yml               # Kafka, Airflow, PostgreSQL services
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
└── .env                             # API keys and config variables
```

---

## Components

### 1. **Streaming with OpenSky + Kafka**

- `bash_streaming.py` runs a loop calling `kafka_producer.py` every 11 seconds (9 AM–4 PM).
- Sends JSON flight data into the `opensky_data` Kafka topic.

### 2. **Kafka Consumer DAG**

- `opensky_kafka_consumer_dag.py`: Pulls latest records from Kafka every 5 minutes (starts at 9:05 AM).
- Outputs to `data/kafka_logs/opensky/`, then runs through the ETL layers.

### 3. **Batch Ingestion from AviationStack**

- `aviationstack_etl_dag.py`: Runs 3x/day (every 4 hours).
- Batches scheduled flight info and processes it through Bronze → Gold.

### 4. **Altitude Alert DAG**

- `opensky_altitude_alert_dag.py`: Uses the same Kafka data but checks for aircraft flying below a configured altitude (e.g., 300 ft).
- Emits logs/alerts for edge-case flight events.

---

## ETL Pipeline (Bronze → Silver → Gold)

- **Bronze**: Raw ingestion (as-is).
- **Silver**: Cleaned + filtered data (null handling, field renaming).
- **Gold**: Business-ready metrics and aggregations (e.g., flights by airline, altitude trends, flight volume heatmaps).

---

## Setup

### Prerequisites

- Docker + Docker Compose
- Python 3.9+
- [OpenSky API key](https://opensky-network.org/) (optional for extended access)
- [AviationStack API key](https://aviationstack.com/)

### Quickstart

```bash
git clone https://github.com/ben-grigsby/FlightFlow.git
cd FlightFlow
cp .env.example .env  # Add your API keys
docker-compose up --build
```

Then visit:
- Airflow UI: http://localhost:8080
- Kafka (via container): `kafka:9092`

---

## ✅ DAG Scheduling Summary

| DAG Name                        | Interval             | Purpose                                 |
|-------------------------------|----------------------|-----------------------------------------|
| `aviationstack_etl_dag`       | Every 4 hours        | Pulls & processes scheduled flight data |
| `opensky_data_streaming_dag`  | Every 11 sec (9–16h) | Streams live flight data into Kafka     |
| `opensky_kafka_consumer_dag`  | Every 5 min (9:05–17)| ETL for streamed data                   |
| `opensky_altitude_alert_dag`  | Every 11 sec (9–16h) | Flags aircraft below critical altitude  |

---

## 📈 Future Additions

- Prometheus + Grafana for monitoring DAG runs, Kafka throughput
- Streamlit dashboard for live aircraft visualizations
- PostgreSQL or Snowflake warehouse integration
- Airflow alert emails and SLAs

---

## 👨‍💻 Author

**Ben Grigsby**  
Statistics & ML Student @ UC Davis  
[GitHub](https://github.com/ben-grigsby) • [LinkedIn](https://linkedin.com/in/ben-grigsby)