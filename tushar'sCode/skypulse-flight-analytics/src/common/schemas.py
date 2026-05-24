from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


flight_fare_event_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("airline", StringType(), True),
        StructField("source_city", StringType(), True),
        StructField("destination_city", StringType(), True),
        StructField("departure_time", StringType(), True),
        StructField("arrival_time", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("total_stops", StringType(), True),
        StructField("route", StringType(), True),
        StructField("date", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("ingestion_source", StringType(), True),
    ]
)

