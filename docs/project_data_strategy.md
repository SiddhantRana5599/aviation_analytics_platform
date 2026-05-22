# Aviation Analytics Data Strategy

## Recommended Project Scope

Project title:

Real-Time Aviation Fare Analytics and Dynamic Price Prediction System

The project should focus on Indian domestic airline fare analytics. Use international/foreign airport data only as optional enrichment, not as the main project scope.

## Data Sources

### 1. SerpAPI Google Flights Data

Use for live/streaming data.

Current file:

`data/processed/flight_prices_2026-05-22.csv`

Purpose:

- Kafka producer input
- Spark Structured Streaming input
- HBase latest fare lookup
- live dashboard panel
- price surge alerts

Do not use SerpAPI as the main ML training dataset because API credits are limited.

### 2. `flight_cleaned.csv`

Use as the main historical batch dataset for price prediction.

Why:

- Looks like real historical airfare data
- Has booking date, journey date, airline, route, stops, duration, lead time, and price
- Good for PySpark ML regression

Main ML target:

`Price`

Useful features:

- `From`
- `To`
- `Airline`
- `Stops`
- `Booking_Hour`
- `Booking_Slot`
- `Lead_Time_Days`
- `Duration_mins`
- journey day/month derived from `Journey_date`

### 3. `flight_clean.csv`

Use as optional secondary/synthetic engineered dataset.

Why:

- Has many ready-made ML features
- Larger than `flight_cleaned.csv`
- Good for testing PySpark ETL and dashboard charts

Use carefully in project explanation because some columns look generated, such as demand level, route popularity, seats available, and peak season.

### 4. `Air_full-Raw.csv`

Use for schedule analytics, not price prediction.

Purpose:

- airline route coverage
- schedule frequency by day of week
- planned departure/arrival analysis
- joining airline + route availability with fare data

This file does not contain price or actual delay labels.

### 5. `airports.csv`

Use as airport dimension/enrichment data.

Purpose:

- airport metadata lookup
- country/region filtering
- latitude/longitude for route maps
- airport type and scheduled service filtering

For the main Indian domestic project, filter this to `iso_country = IN`.

## Delay Prediction Decision

Do not make delay prediction the main ML objective unless you add a true delay dataset or another API that provides actual arrival/departure delay.

The current CSV files do not contain reliable delay labels such as:

- scheduled departure
- actual departure
- scheduled arrival
- actual arrival
- departure delay minutes
- arrival delay minutes

Without a delay target column, a delay prediction model would be weak and hard to defend in viva.

Recommended ML objective:

Flight price prediction.

Optional analytics:

- fare surge detection
- cheapest airline by route
- booking window analysis
- airline route coverage
- route-wise fare trends

## Batch vs Streaming Plan

### Batch Layer

Use Kaggle/historical CSV files:

- `flight_cleaned.csv` as primary batch ML dataset
- `flight_clean.csv` as optional large analytical dataset
- `Air_full-Raw.csv` as schedule dimension
- `airports.csv` as airport dimension

Batch jobs:

- load CSV to HDFS
- create Hive external tables
- clean and transform using PySpark
- train price prediction model using PySpark MLlib
- write aggregated outputs to Hive/dashboard tables

### Streaming Layer

Use SerpAPI CSV/raw JSON and future SerpAPI calls.

Streaming jobs:

- Python scraper fetches latest fares
- Kafka producer publishes flattened rows to `flight_prices_live`
- Spark Structured Streaming reads from Kafka
- compute latest fare, price changes, surge flags
- write latest records to HBase
- write append-only stream to HDFS/Hive

## SerpAPI Credit-Saving Strategy

You have limited SerpAPI credits, so avoid frequent full-route collection.

Recommended schedule during development:

- use `--dry-run` while testing
- collect only 3 to 5 routes initially
- collect only 1 or 2 future travel dates
- run manually, not every 30 minutes, until Kafka/Spark is ready

For final demo:

- run one collection before presentation
- optionally run a second collection after 30 to 60 minutes to show price change

Do not scrape all 10 routes x 7 days repeatedly until the full pipeline is stable.

## Future API Extension

The project can later add another API cleanly by creating a second ingestion module.

Possible future APIs:

- Aviationstack for flight status and delay-like operational data
- OpenSky Network for live aircraft movement
- Aviation Edge for schedules/status
- Amadeus APIs for richer travel data

Keep the normalized output schema similar to the current flattened CSV so Kafka, Spark, Hive, and dashboard layers do not need major rewrites.

