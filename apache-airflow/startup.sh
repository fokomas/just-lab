#!/bin/bash

# Set up directories
AIRFLOW_DIR=$(pwd)/airflow
DATABASES_DIR=$(pwd)/databases

# Navigate to Airflow directory and set up environment variables
cd "$AIRFLOW_DIR" || { echo "Airflow directory not found!"; exit 1; }
printf "AIRFLOW_UID=%s\nAIRFLOW_GID=0\n" "$(id -u)"> .env
echo "Initializing Airflow..."
docker compose up airflow-init || { echo "Airflow initialization failed!"; exit 1; }

# Start Airflow services
echo "Starting Airflow services..."
docker compose up -d || { echo "Failed to start Airflow services!"; exit 1; }

# Start database services for ETL
cd "$DATABASES_DIR" || { echo "Databases directory not found!"; exit 1; }
echo "Starting database services..."
docker compose up -d || { echo "Failed to start databases!"; exit 1; }

echo "All services are up and running!"
