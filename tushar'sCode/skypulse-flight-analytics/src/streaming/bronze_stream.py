from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json

from src.common.schemas import flight_fare_event_schema


def read_kafka_events(spark: SparkSession, config: dict) -> DataFrame:
    kafka_config = config["kafka"]
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("subscribe", kafka_config["topic"])
        .option("startingOffsets", "latest")
        .load()
    )


def parse_bronze_events(kafka_frame: DataFrame) -> DataFrame:
    return (
        kafka_frame.selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS payload", "timestamp AS kafka_timestamp")
        .withColumn("event", from_json(col("payload"), flight_fare_event_schema))
        .select("kafka_key", "payload", "kafka_timestamp", "event.*")
        .withColumn("bronze_ingested_at", current_timestamp())
    )


def write_bronze_stream(bronze_frame: DataFrame, config: dict):
    checkpoint = f"{config['streaming']['checkpoint_base_path']}/bronze"
    return (
        bronze_frame.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("path", config["delta"]["bronze_path"])
        .trigger(processingTime="30 seconds")
        .start()
    )

