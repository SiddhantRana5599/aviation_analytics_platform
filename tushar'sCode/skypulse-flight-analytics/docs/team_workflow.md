# Team Workflow

## Branching

- `main`: final stable demo
- `develop`: integration branch
- `feature/kafka-streaming`: Kafka producer and streaming configs
- `feature/ml-pipeline`: feature engineering and ML training
- `feature/streamlit-ui`: dashboard
- `feature/airflow-dag`: Airflow orchestration

## Ownership

Member 1 owns Kafka, Airflow, Bronze, and Silver.

Member 2 owns Gold, ML, Streamlit, and documentation.

## Merge Practices

- Pull from `develop` before starting work.
- Keep pull requests small.
- Avoid editing the same file at the same time.
- Use README and docs as shared contracts.
- Merge to `main` only after the full demo flow works.

