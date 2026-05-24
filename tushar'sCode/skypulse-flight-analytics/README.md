# SkyPulse - Real-Time Flight Fare Intelligence Platform

SkyPulse is an industry-style Big Data project for real-time flight fare analytics and ticket price prediction. It uses simulated streaming from a Kaggle CSV, Kafka, Spark Structured Streaming, Delta Lake, Databricks, Airflow, Spark SQL, Streamlit, and a Medallion Architecture.

The project is intentionally scoped for a 2-member team and a 1-week final-year project timeline. It demonstrates real enterprise data engineering concepts without Kubernetes, heavy cloud infrastructure, or complex DevOps.

## Why This Architecture

Kafka simulates real-time fare events from historical CSV data. Spark Structured Streaming processes events continuously, applies watermarking and sliding windows, and writes Delta Lake tables. Databricks is used because it gives Spark, Delta, notebooks, and SQL in one beginner-friendly environment. Airflow orchestrates scheduled batch jobs and ML retraining. Streamlit gives a lightweight professional dashboard for demo and viva.

Tradeoff: Databricks Community Edition is excellent for Spark and Delta, but it does not host Kafka or Airflow. Run Kafka, the producer, Airflow, and Streamlit locally. Run Spark notebooks/jobs in Databricks.

## Architecture

```text
Kaggle CSV
   |
   v
Local Kafka Producer
   |
   v
Kafka Topic: flight_fares_stream
   |
   v
Databricks Spark Structured Streaming
   |
   +--> Bronze Delta: raw append-only events
   |
   +--> Silver Delta: cleaned, deduplicated, standardized events
   |
   +--> Gold Delta: KPIs, route analytics, sliding-window fare trends, ML features
   |
   +--> ML Pipeline: fare prediction model
   |
   v
Streamlit Dashboard + Spark SQL Analytics
```

## Folder Structure

```text
SkyPulse/
|-- data/
|   |-- sample/
|-- notebooks/
|-- airflow/
|   |-- dags/
|-- kafka/
|-- streamlit_app/
|-- src/
|   |-- batch/
|   |-- common/
|   |-- ml/
|   |-- streaming/
|-- configs/
|-- docs/
|-- tests/
|-- requirements.txt
|-- README.md
|-- .gitignore
```

## Dataset

Use a Kaggle Indian flight fare dataset that contains major Indian cities. The selected dataset must include Pune, Mumbai, Delhi, Bangalore, Hyderabad, Chennai, Kolkata, and Ahmedabad.

Expected CSV columns can be mapped in `configs/pipeline_config.yaml`:

```text
airline, source_city, destination_city, departure_time, arrival_time,
duration, total_stops, price, date
```

Place the dataset at:

```text
data/raw/flight_fares.csv
```

A tiny schema-compatible sample is included at `data/sample/flight_fares_sample.csv` for quick validation.

## Implementation Plan

1. Create repo structure and install dependencies.
2. Download Kaggle CSV and validate required cities.
3. Start local Kafka and create `flight_fares_stream`.
4. Run `kafka/flight_fare_producer.py` to push CSV rows gradually into Kafka.
5. Upload source code/configs to Databricks or connect GitHub repo.
6. Run Databricks notebooks in order:
   - `01_setup_and_external_tables.md`
   - `02_bronze_streaming.md`
   - `03_silver_streaming.md`
   - `04_gold_streaming_and_sql.md`
   - `05_ml_training.md`
7. Run Streamlit locally to visualize Gold tables or exported sample outputs.
8. Use Airflow locally to orchestrate producer checks, batch quality checks, and ML retraining commands.

## Day-Wise Roadmap

| Day | Member 1 | Member 2 | Output |
| --- | --- | --- | --- |
| 1 | Repo setup, Kafka setup | Dataset download, schema mapping | Clean project skeleton |
| 2 | Kafka producer | Databricks setup, Bronze table | Streaming raw ingestion |
| 3 | Bronze/Silver streaming | Silver validations | Clean Delta layer |
| 4 | Airflow DAG | Gold KPIs and SQL | Orchestrated pipeline |
| 5 | Streaming monitor | ML feature table and model | Prediction pipeline |
| 6 | Integration testing | Streamlit dashboard | End-to-end demo |
| 7 | Docs, viva prep | README, resume bullets | Final submission |

## Local Setup

Create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Kafka options:

- Use local Kafka binaries if already installed.
- Use Docker only if your local setup becomes painful. Docker is optional, not core architecture.

Create the Kafka topic:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic flight_fares_stream --partitions 3 --replication-factor 1
```

Run producer:

```bash
python kafka/flight_fare_producer.py --config configs/pipeline_config.yaml
```

Expected producer output:

```text
Published 100 fare events to flight_fares_stream
```

## Databricks Execution

1. Create a Databricks Community Edition cluster.
2. Install libraries:
   - `delta-spark`
   - `kafka-python`
3. Import notebooks from `notebooks/`.
4. Upload `src/` and `configs/`, or connect GitHub.
5. Use DBFS/Delta paths from `configs/pipeline_config.yaml`.

Run order:

```text
01_setup_and_external_tables
02_bronze_streaming
03_silver_streaming
04_gold_streaming_and_sql
05_ml_training
```

## Delta External Tables

Example:

```sql
CREATE TABLE IF NOT EXISTS skypulse_gold.route_price_windows
USING DELTA
LOCATION '/tmp/skypulse/gold/route_price_windows';
```

The project uses external Delta locations so data lifecycle and table metadata are separate, which is closer to enterprise lakehouse practice.

## Streamlit

Run locally:

```bash
streamlit run streamlit_app/app.py
```

If Databricks connectivity is unavailable, the dashboard can read exported CSV samples from `data/exports/`. This keeps the final demo resilient.

## Airflow

Airflow is used for scheduled orchestration:

- dataset validation
- producer health checks
- batch Gold refresh
- ML retraining trigger

Start Airflow locally after installing requirements:

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate
airflow users create --username admin --firstname Sky --lastname Pulse --role Admin --email admin@example.com --password admin
airflow standalone
```

Copy or symlink `airflow/dags/skypulse_pipeline_dag.py` into your Airflow DAGs folder if needed.

## Git Strategy

Branches:

- `main`: stable demo-ready code
- `develop`: integration branch
- `feature/kafka-streaming`: producer and Kafka configs
- `feature/ml-pipeline`: feature engineering and ML model
- `feature/streamlit-ui`: dashboard
- `feature/airflow-dag`: orchestration

Ownership:

- Member 1: Kafka, Airflow, Bronze/Silver streaming
- Member 2: Gold layer, ML, Streamlit, documentation

Merge rule: open small pull requests into `develop`, test notebook order, then merge `develop` into `main` before final demo.

## Expected Demo Flow

1. Show architecture diagram from `docs/architecture.md`.
2. Start Kafka and producer.
3. Show Bronze Delta table receiving records.
4. Show Silver cleaned and deduplicated records.
5. Show Gold sliding-window analytics with watermarking.
6. Open Streamlit dashboard.
7. Run price prediction page.
8. Explain Airflow DAG and Git workflow.

## Troubleshooting

Kafka connection refused:

- Confirm broker is running on `localhost:9092`.
- Confirm topic exists.
- Check `configs/pipeline_config.yaml`.

No rows in Bronze:

- Confirm the producer is publishing.
- Confirm Databricks can reach the Kafka broker. If not, run Spark locally for streaming demo or export Kafka data for Databricks batch fallback.

Databricks cannot connect to local Kafka:

- This is common in Community Edition because local laptop services are not reachable from Databricks. For demo, run Spark streaming locally or use a reachable Kafka broker. Keep Databricks notebooks for Delta, SQL, and ML demonstration.

Streamlit has no data:

- Export Gold tables to `data/exports/` as CSV.
- Use the fallback mode in `streamlit_app/data_access.py`.

## Deliverables

All requested deliverables are included:

- architecture and system design: `docs/architecture.md`
- implementation plan and roadmap: this README and `docs/project_plan.md`
- Kafka producer: `kafka/flight_fare_producer.py`
- Spark Structured Streaming: `src/streaming/`
- Databricks notebook structure: `notebooks/`
- Airflow DAG: `airflow/dags/skypulse_pipeline_dag.py`
- Delta schemas: `docs/delta_schemas.md`
- Streamlit dashboard: `streamlit_app/`
- ML pipeline: `src/ml/train_price_model.py`
- Git strategy: this README and `docs/team_workflow.md`
- interview and presentation prep: `docs/project_plan.md`, `docs/architecture.md`
- resume bullets: `docs/resume_bullets.md`
- deployment instructions: `docs/deployment.md`
