# Architecture

SkyPulse follows a lightweight lakehouse architecture.

## Bronze

Bronze stores raw Kafka JSON events as Delta. Transformations are minimal: parse payload, keep Kafka metadata, and add ingestion timestamp. This preserves the original event for replay and debugging.

## Silver

Silver applies data quality rules:

- required `event_id`
- positive `price`
- positive `duration`
- standardized city casing
- route reconstruction
- duplicate removal by `event_id`
- watermarking on `event_time`

## Gold

Gold contains business-ready tables:

- `route_price_windows`: streaming 15-minute windows sliding every 5 minutes
- `route_daily_metrics`: historical route and airline KPIs
- `ml_fare_features`: model-ready feature table

## System Design Explanation

Kafka decouples ingestion from processing. Spark Structured Streaming gives exactly-once-style processing with checkpoints and Delta sinks. Delta Lake provides ACID tables and schema evolution. External Delta tables make data reusable from SQL, notebooks, and dashboards. Airflow coordinates scheduled work while Databricks handles distributed Spark execution.

## Future Extensibility

The same flow can ingest airline APIs, travel search APIs, or scraper outputs by publishing all records into a common Kafka event schema.

