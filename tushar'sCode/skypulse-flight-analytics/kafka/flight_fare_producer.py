from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from kafka import KafkaProducer

from src.common.config import kafka_settings, load_config


def normalize_city(value: Any) -> str:
    return str(value).strip().title()


def build_event(row: pd.Series, mapping: dict[str, str]) -> dict[str, Any]:
    source_city = normalize_city(row[mapping["source_city"]])
    destination_city = normalize_city(row[mapping["destination_city"]])
    event_time = datetime.now(timezone.utc).isoformat()

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": event_time,
        "airline": str(row[mapping["airline"]]).strip(),
        "source_city": source_city,
        "destination_city": destination_city,
        "departure_time": str(row[mapping["departure_time"]]).strip(),
        "arrival_time": str(row[mapping["arrival_time"]]).strip(),
        "duration": float(row[mapping["duration"]]),
        "total_stops": str(row[mapping["total_stops"]]).strip(),
        "route": f"{source_city}-{destination_city}",
        "date": str(row[mapping["date"]]).strip(),
        "price": float(row[mapping["price"]]),
        "ingestion_source": "kaggle_csv_simulator",
    }


def validate_required_cities(frame: pd.DataFrame, mapping: dict[str, str], required_cities: list[str]) -> None:
    source_values = set(frame[mapping["source_city"]].dropna().map(normalize_city).unique())
    destination_values = set(frame[mapping["destination_city"]].dropna().map(normalize_city).unique())
    available_cities = source_values | destination_values
    missing = sorted(set(required_cities) - available_cities)
    if missing:
        raise ValueError(f"Dataset is missing required cities: {missing}")


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=20,
        retries=3,
    )


def run(config_path: str) -> None:
    config = load_config(config_path)
    settings = kafka_settings(config)
    producer_config = config["producer"]
    mapping = config["schema_mapping"]

    csv_path = Path(producer_config["source_csv_path"])
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    frame = pd.read_csv(csv_path)
    validate_required_cities(frame, mapping, producer_config["required_cities"])

    max_rows = int(producer_config.get("max_rows", 0))
    if max_rows > 0:
        frame = frame.head(max_rows)

    producer = create_producer(settings.bootstrap_servers)
    interval = float(producer_config.get("publish_interval_seconds", 0.5))

    published_count = 0
    for _, row in frame.iterrows():
        event = build_event(row, mapping)
        producer.send(settings.topic, key=event["route"], value=event)
        published_count += 1

        if published_count % 100 == 0:
            producer.flush()
            print(f"Published {published_count} fare events to {settings.topic}")

        time.sleep(interval)

    producer.flush()
    producer.close()
    print(f"Completed publishing {published_count} fare events to {settings.topic}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate real-time flight fare events into Kafka.")
    parser.add_argument("--config", default="configs/pipeline_config.yaml")
    args = parser.parse_args()
    run(args.config)

