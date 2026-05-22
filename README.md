# Aviation Analytics

First milestone for the CDAC DBDA aviation analytics project: collect Google Flights data from SerpAPI, store raw JSON, and create a flattened CSV for Hadoop/Hive/PySpark/Kafka work.

## What This Scraper Collects

The scraper tracks 10 Indian flight routes and writes:

- Raw SerpAPI JSON under `data/raw/flights/<scrape_date>/<route>/`
- Flattened analytics CSV under `data/processed/flight_prices_<date>.csv`

The flattened CSV includes route, airline, flight number, departure/arrival time, duration, stops, price, day-of-week, weekend/holiday flags, and price labels.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` and add your SerpAPI key:

```text
SERPAPI_API_KEY=your_serpapi_api_key_here
```

## Check Planned API Calls

This does not spend API credits:

```powershell
python src\serpapi_flight_scraper.py --dry-run --days-count 1
```

## Run Collection

Collect tomorrow's flights for all 10 routes:

```powershell
python src\serpapi_flight_scraper.py --days-count 1
```

Collect the next 7 travel dates for all routes:

```powershell
python src\serpapi_flight_scraper.py --days-ahead 1 --days-count 7
```

Use deeper Google Flights matching when precision matters:

```powershell
python src\serpapi_flight_scraper.py --days-count 1 --deep-search
```

## CSV Schema

```text
scrape_timestamp, search_id, source_airport, destination_airport, source_city,
destination_city, route, travel_date, departure_datetime, arrival_datetime,
departure_hour, arrival_hour, airline, flight_number, aircraft, travel_class,
duration_minutes, stops, stop_airports, price, currency, days_left, day_of_week,
is_weekend, is_holiday, price_category, previous_price, price_change,
price_change_percent, surge_flag, cheapest_flag
```

## Next Project Step

After this works locally, the next clean step is to add a Kafka producer that sends each flattened row to a `flight_prices_live` topic while still storing raw JSON for batch processing.

