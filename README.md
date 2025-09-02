# Flight and Weather Streaming ETL Pipeline

## Overview

This project builds a streaming ETL pipeline to integrate and analyze real-time flight data from multiple sources. It pulls data from:

- **OpenSky Network**: For real-time flight state vectors.
- **AviationStack API**: For commercial flight schedule and status data.
- **OpenWeather API** (optional): For weather enrichment data based on flight location.

The pipeline joins, transforms, and stores the data in a PostgreSQL database for further analysis. The architecture is modular, allowing for new data sources or outputs to be added easily.

---

## Features

- Kafka-based streaming architecture
- Modular producers and consumers
- PostgreSQL integration
- Data enrichment via external APIs
- Schema-based and structured ingestion
- Potential for visualization and dashboarding

---

## Project Structure

```text
.
├── docker-compose.yml
├── Dockerfile
├── dags/
│   ├── aviationstack/
│   │   ├── test_producer.py
│   │   └── test_consumer.py
│   └── opensky/
│       ├── test_producer.py
│       ├── test_consumer.py
│       └── utils.py
├── etl/
│   ├── postgre.py
│   ├── SQL_functions.py
│   └── sql_test.py
├── infra/
│   └── init_db.sql
├── tests/
│   └── sql_test.py
└── README.md
```

---

## Pipeline Architecture

```text
+----------------------+
|   OpenSky API        |
+----------------------+
           |
           v
+----------------------+
|  Kafka Producer      |
+----------------------+
           |
           v
+----------------------+
|   Kafka Topic        |
+----------------------+
           |
           v
+----------------------+
|  Kafka Consumer      |
+----------------------+
           |
           v
+----------------------+       +----------------------+
| Transformed Flight   |<----->| AviationStack API     |
| Data (Merged, Clean) |       +----------------------+
           |
           v
+----------------------+
|  Optional Enrichment |
|    via OpenWeather   |
+----------------------+
           |
           v
+----------------------+
|   PostgreSQL DB      |
+----------------------+
```

---

## Planned Enhancements

- Add Avro + Schema Registry for better serialization
- Add Streamlit dashboard for flight tracking and filtering
- Use Apache Airflow for orchestration
- Optionally integrate Apache Iceberg or Delta Lake for large-scale historical storage

---

## Setup & Deployment

1. Clone the repo:
   ```bash
   git clone https://github.com/YOUR_USERNAME/flightweather.git
   cd flightweather
   ```

2. Start the containers:
   ```bash
   docker-compose up --build
   ```

3. Run producers:
   ```bash
   docker exec -it stock_dev python3 dags/opensky/test_producer.py
   docker exec -it stock_dev python3 dags/aviationstack/test_producer.py
   ```

4. Run consumers:
   ```bash
   docker exec -it stock_dev python3 dags/opensky/test_consumer.py
   docker exec -it stock_dev python3 dags/aviationstack/test_consumer.py
   ```

---

## Database Initialization

Your schema is created automatically by executing the SQL file located in:

```text
infra/init_db.sql
```

---

Let me know if you want a version that includes weather dashboarding, Airflow orchestration, or file system enhancements.