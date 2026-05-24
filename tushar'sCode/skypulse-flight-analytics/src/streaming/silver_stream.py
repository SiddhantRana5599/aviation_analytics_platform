from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, initcap, sha2, to_date


def read_bronze_stream(spark: SparkSession, config: dict) -> DataFrame:
    return spark.readStream.format("delta").load(config["delta"]["bronze_path"])


def clean_silver_events(bronze_frame: DataFrame, config: dict) -> DataFrame:
    watermark_delay = config["streaming"]["watermark_delay"]

    cleaned = (
        bronze_frame.withWatermark("event_time", watermark_delay)
        .filter(col("event_id").isNotNull())
        .filter(col("price") > 0)
        .filter(col("duration") > 0)
        .withColumn("source_city", initcap(col("source_city")))
        .withColumn("destination_city", initcap(col("destination_city")))
        .withColumn("travel_date", to_date(col("date")))
        .withColumn("route", concat_ws("-", col("source_city"), col("destination_city")))
        .withColumn("silver_updated_at", current_timestamp())
    )

    return cleaned.dropDuplicates(["event_id"])


def write_silver_stream(silver_frame: DataFrame, config: dict):
    checkpoint = f"{config['streaming']['checkpoint_base_path']}/silver"
    return (
        silver_frame.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("path", config["delta"]["silver_path"])
        .trigger(processingTime="30 seconds")
        .start()
    )

