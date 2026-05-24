from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic: str
    security_protocol: str = "PLAINTEXT"


def kafka_settings(config: dict[str, Any]) -> KafkaSettings:
    kafka_config = config["kafka"]
    return KafkaSettings(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        topic=kafka_config["topic"],
        security_protocol=kafka_config.get("security_protocol", "PLAINTEXT"),
    )

