from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, max as spark_max, min as spark_min, window


def read_silver_stream(spark: SparkSession, config: dict) -> DataFrame:
    return spark.readStream.format("delta").load(config["delta"]["silver_path"])


def build_route_price_windows(silver_frame: DataFrame, config: dict) -> DataFrame:
    streaming_config = config["streaming"]

    return (
        silver_frame.withWatermark("event_time", streaming_config["watermark_delay"])
        .groupBy(
            window(col("event_time"), streaming_config["window_duration"], streaming_config["slide_duration"]),
            col("airline"),
            col("source_city"),
            col("destination_city"),
            col("route"),
        )
        .agg(
            count("*").alias("fare_event_count"),
            avg("price").alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            avg("duration").alias("avg_duration"),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )


def write_route_price_windows(gold_frame: DataFrame, config: dict):
    checkpoint = f"{config['streaming']['checkpoint_base_path']}/gold_route_windows"
    return (
        gold_frame.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("path", config["delta"]["gold_route_window_path"])
        .trigger(processingTime="1 minute")
        .start()
    )

