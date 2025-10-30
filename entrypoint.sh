#!/bin/bash

# Wait for PostgreSQL to be ready
echo "Waiting for postgres..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "PostgreSQL is up."

# Initialize the Airflow database
airflow db init

# Create admin user if it doesn't already exist
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# Keep container alive (optional, for debugging)
tail -f /dev/null