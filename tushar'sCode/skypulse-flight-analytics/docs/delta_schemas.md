# Delta Table Schemas

## Bronze: `skypulse_bronze.flight_fare_events`

| Column | Type | Description |
| --- | --- | --- |
| event_id | string | Unique event id |
| event_time | timestamp | Simulated event timestamp |
| airline | string | Airline name |
| source_city | string | Origin city |
| destination_city | string | Destination city |
| departure_time | string | Time bucket |
| arrival_time | string | Time bucket |
| duration | double | Flight duration |
| total_stops | string | Stops category |
| route | string | Source-destination route |
| date | string | Travel date |
| price | double | Ticket price |
| payload | string | Raw Kafka payload |
| kafka_timestamp | timestamp | Kafka ingestion timestamp |
| bronze_ingested_at | timestamp | Bronze write timestamp |

## Silver: `skypulse_silver.flight_fare_events_clean`

Bronze columns plus:

| Column | Type | Description |
| --- | --- | --- |
| travel_date | date | Parsed travel date |
| silver_updated_at | timestamp | Silver write timestamp |

## Gold: `skypulse_gold.route_price_windows`

| Column | Type | Description |
| --- | --- | --- |
| window | struct | Sliding window start/end |
| airline | string | Airline |
| source_city | string | Origin |
| destination_city | string | Destination |
| route | string | Route |
| fare_event_count | long | Events in window |
| avg_price | double | Average fare |
| min_price | double | Minimum fare |
| max_price | double | Maximum fare |
| avg_duration | double | Average duration |
| gold_updated_at | timestamp | Gold write timestamp |

