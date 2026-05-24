# Deployment Instructions

## Local Components

Run locally:

- Kafka broker
- Kafka producer
- Airflow
- Streamlit

## Databricks Components

Run in Databricks:

- Bronze streaming notebook
- Silver streaming notebook
- Gold streaming and SQL notebook
- ML training notebook

## Demo-Ready Fallback

If Databricks cannot reach local Kafka:

1. Run producer locally to prove event simulation.
2. Use a local Spark session or uploaded CSV to create Delta tables.
3. Export Gold tables to `data/exports/`.
4. Run Streamlit from exported CSVs.

This fallback still demonstrates the full design and avoids last-minute network issues.

