# Databricks Notebook 02 - Bronze Streaming

## Purpose

Read Kafka events, parse JSON, and write raw append-only events to Bronze Delta.

```python
import sys
sys.path.append("/Workspace/Repos/<your-user>/skypulse-flight-analytics")

from src.common.config import load_config
from src.streaming.bronze_stream import parse_bronze_events, read_kafka_events, write_bronze_stream

config = load_config("/Workspace/Repos/<your-user>/skypulse-flight-analytics/configs/pipeline_config.yaml")
kafka_df = read_kafka_events(spark, config)
bronze_df = parse_bronze_events(kafka_df)
query = write_bronze_stream(bronze_df, config)
```

Expected output: rows continuously appear in `skypulse_bronze.flight_fare_events`.
