from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, dayofweek, datediff, month, to_date


def build_daily_route_metrics(spark: SparkSession, config: dict) -> None:
    silver = spark.read.format("delta").load(config["delta"]["silver_path"])

    daily_metrics = (
        silver.withColumn("travel_date", to_date(col("date")))
        .groupBy("travel_date", "airline", "source_city", "destination_city", "route")
        .agg(
            count("*").alias("total_fare_events"),
            avg("price").alias("avg_price"),
            avg("duration").alias("avg_duration"),
        )
    )

    daily_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        config["delta"]["gold_route_daily_path"]
    )


def build_ml_features(spark: SparkSession, config: dict) -> None:
    silver = spark.read.format("delta").load(config["delta"]["silver_path"])

    features = (
        silver.withColumn("travel_date", to_date(col("date")))
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("days_to_departure", datediff(col("travel_date"), col("event_date")))
        .withColumn("travel_month", month(col("travel_date")))
        .withColumn("travel_day_of_week", dayofweek(col("travel_date")))
        .filter(col("days_to_departure").isNotNull())
        .filter(col("days_to_departure") >= 0)
        .select(
            "airline",
            "source_city",
            "destination_city",
            "departure_time",
            "total_stops",
            "duration",
            "days_to_departure",
            "travel_month",
            "travel_day_of_week",
            "price",
        )
    )

    features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        config["delta"]["gold_ml_features_path"]
    )

