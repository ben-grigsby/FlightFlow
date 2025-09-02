# Weather & Flight Streaming ETL Pipeline

This project implements a real-time data pipeline that ingests live flight data from the OpenSky Network API, enriches it with weather data (optional), and stores the results in a PostgreSQL database using a Kafka-based streaming architecture.

## Overview

The purpose of this pipeline is to simulate a production-grade data engineering workflow that includes:

- Real-time data ingestion from a public API (OpenSky)
- Kafka for message queuing and streaming
- PostgreSQL for structured data storage
- Docker for containerization and orchestration
- (Optional) Weather enrichment using OpenWeather API

## Architecture

```text
+------------------+       +-------------------+       +---------------------+       +--------------------+
|   OpenSky API    | --->  |   Kafka Producer   | --->  |   Kafka Consumer     | --->  |   PostgreSQL (DB)   |
+------------------+       +-------------------+       +---------------------+       +--------------------+
                                                          |
                                                          v
                                                (Optional) Weather API
```

- **Producer**: Pulls data from OpenSky and sends JSON messages to Kafka topic.
- **Consumer**: Listens to Kafka topic, parses data, and inserts it into `flight_data` table.
- **PostgreSQL**: Stores the structured flight data for querying and analysis.

## Technologies Used

| Component         | Tool/Service           |
|------------------|------------------------|
| Streaming        | Apache Kafka           |
| Message Broker   | Kafka Topics           |
| Storage          | PostgreSQL             |
| Containerization | Docker + Docker Compose|
| Language         | Python                 |
| Scheduling (optional) | Airflow/Cron       |
| Weather API (optional) | OpenWeather API   |

## Database Schema

### Table: `flight_data`

| Column           | Type             |
|------------------|------------------|
| icao24           | TEXT             |
| callsign         | TEXT             |
| origin_country   | TEXT             |
| time_position    | BIGINT           |
| last_contact     | BIGINT           |
| longitude        | DOUBLE PRECISION |
| latitude         | DOUBLE PRECISION |
| baro_altitude    | DOUBLE PRECISION |
| on_ground        | BOOLEAN          |
| velocity         | DOUBLE PRECISION |
| true_track       | DOUBLE PRECISION |
| vertical_rate    | DOUBLE PRECISION |
| sensors          | BIGINT           |
| geo_altitude     | DOUBLE PRECISION |
| squawk           | TEXT             |
| spi              | BOOLEAN          |
| position_source  | BIGINT           |
| category         | BIGINT           |

## File Structure

```text
.
├── docker-compose.yml
├── Dockerfile
├── dags/
│   └── opensky/
│       ├── producer.py
│       ├── consumer.py
│       └── utils.py
├── etl/
│   ├── postgres.py
│   └── sql/
│       └── init_db.sql
├── infra/
│   └── init_db.sql
├── tests/
│   └── sql_test.py
└── README.md
```

## How to Run

1. **Clone the repo**
   ```bash
   git clone https://github.com/yourusername/weather-flight-streaming.git
   cd weather-flight-streaming
   ```

2. **Start the environment**
   ```bash
   docker-compose up --build
   ```

3. **Run producer manually (for now)**
   ```bash
   docker exec -it <your_kafka_container> python3 dags/opensky/producer.py
   ```

4. **Run consumer manually**
   ```bash
   docker exec -it <your_kafka_container> python3 dags/opensky/consumer.py
   ```

5. **Query PostgreSQL**
   ```sql
   SELECT * FROM flight_data LIMIT 5;
   ```

## Optional Extensions

- Integrate OpenWeather API to enrich flight data based on lat/lon
- Automate ingestion with Airflow
- Expose metrics or dashboards via Streamlit or Superset
- Use Avro or Protobuf for schema enforcement
- Add historical storage layer using Parquet + Iceberg (if scaled up)

## Future Work

- Build interactive dashboards with real-time tracking
- Add retry logic and error handling for flaky API calls
- Migrate to cloud-native stack (AWS MSK + RDS + Lambda)