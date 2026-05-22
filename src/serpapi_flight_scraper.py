from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv() -> bool:
        env_path = Path(".env")
        if not env_path.exists():
            return False

        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return True


SERPAPI_ENDPOINT = "https://serpapi.com/search.json"

CSV_COLUMNS = [
    "scrape_timestamp",
    "search_id",
    "source_airport",
    "destination_airport",
    "source_city",
    "destination_city",
    "route",
    "travel_date",
    "departure_datetime",
    "arrival_datetime",
    "departure_hour",
    "arrival_hour",
    "airline",
    "flight_number",
    "aircraft",
    "travel_class",
    "duration_minutes",
    "stops",
    "stop_airports",
    "price",
    "currency",
    "days_left",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "price_category",
    "previous_price",
    "price_change",
    "price_change_percent",
    "surge_flag",
    "cheapest_flag",
]

INDIA_PUBLIC_HOLIDAYS_2026 = {
    "2026-01-26",  # Republic Day
    "2026-03-04",  # Holi
    "2026-08-15",  # Independence Day
    "2026-10-02",  # Gandhi Jayanti
    "2026-11-08",  # Diwali
    "2026-12-25",  # Christmas
}


@dataclass(frozen=True)
class Route:
    source_airport: str
    source_city: str
    destination_airport: str
    destination_city: str

    @property
    def code(self) -> str:
        return f"{self.source_airport}_{self.destination_airport}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect Google Flights prices from SerpAPI and flatten them for aviation analytics."
    )
    parser.add_argument("--routes-file", default="config/routes_india.json")
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--days-ahead", type=int, default=1, help="First travel date offset from today.")
    parser.add_argument("--days-count", type=int, default=7, help="Number of travel dates per route.")
    parser.add_argument("--currency", default="INR")
    parser.add_argument("--country", default="in", help="SerpAPI Google country parameter, e.g. in/us.")
    parser.add_argument("--language", default="en")
    parser.add_argument("--travel-class", type=int, default=1, help="1=economy in Google Flights.")
    parser.add_argument("--adults", type=int, default=1)
    parser.add_argument("--deep-search", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--dry-run", action="store_true", help="Print planned requests without calling SerpAPI.")
    return parser.parse_args()


def load_routes(path: Path) -> list[Route]:
    with path.open("r", encoding="utf-8") as file:
        rows = json.load(file)
    return [Route(**row) for row in rows]


def travel_dates(days_ahead: int, days_count: int) -> list[date]:
    start = date.today() + timedelta(days=days_ahead)
    return [start + timedelta(days=offset) for offset in range(days_count)]


def build_params(
    route: Route,
    travel_date: date,
    api_key: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "engine": "google_flights",
        "departure_id": route.source_airport,
        "arrival_id": route.destination_airport,
        "outbound_date": travel_date.isoformat(),
        "type": 2,  # one-way
        "currency": args.currency,
        "gl": args.country,
        "hl": args.language,
        "travel_class": args.travel_class,
        "adults": args.adults,
        "api_key": api_key,
    }
    if args.deep_search:
        params["deep_search"] = "true"
    if args.no_cache:
        params["no_cache"] = "true"
    return params


def fetch_flights(params: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(SERPAPI_ENDPOINT, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(payload["error"])
    return payload


def save_raw_json(payload: dict[str, Any], output_dir: Path, route: Route, travel_date: date) -> Path:
    scrape_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_dir = output_dir / "raw" / "flights" / scrape_date / route.code
    raw_dir.mkdir(parents=True, exist_ok=True)

    search_id = payload.get("search_metadata", {}).get("id", "no_search_id")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = raw_dir / f"{travel_date.isoformat()}_{timestamp}_{search_id}.json"

    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False)
    return path


def parse_google_datetime(value: str | None, fallback_date: date) -> datetime | None:
    if not value:
        return None

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %I:%M %p", "%H:%M", "%I:%M %p"):
        try:
            parsed = datetime.strptime(value, fmt)
            if parsed.year == 1900:
                return datetime.combine(fallback_date, parsed.time())
            return parsed
        except ValueError:
            continue
    return None


def parse_price(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else None


def price_category(price: int | None) -> str:
    if price is None:
        return ""
    if price <= 5000:
        return "low"
    if price <= 10000:
        return "medium"
    return "high"


def flatten_payload(
    payload: dict[str, Any],
    route: Route,
    travel_date: date,
    currency: str,
) -> list[dict[str, Any]]:
    search_id = payload.get("search_metadata", {}).get("id", "")
    scrape_timestamp = datetime.now(timezone.utc).isoformat()
    itineraries = []
    itineraries.extend(payload.get("best_flights", []) or [])
    itineraries.extend(payload.get("other_flights", []) or [])

    rows: list[dict[str, Any]] = []
    for itinerary in itineraries:
        segments = itinerary.get("flights", []) or []
        if not segments:
            continue

        first_segment = segments[0]
        last_segment = segments[-1]

        departure_airport = first_segment.get("departure_airport", {})
        arrival_airport = last_segment.get("arrival_airport", {})
        departure_dt = parse_google_datetime(departure_airport.get("time"), travel_date)
        arrival_dt = parse_google_datetime(arrival_airport.get("time"), travel_date)
        price = parse_price(itinerary.get("price"))

        airlines = sorted({segment.get("airline", "") for segment in segments if segment.get("airline")})
        flight_numbers = [segment.get("flight_number", "") for segment in segments if segment.get("flight_number")]
        aircraft = sorted({segment.get("airplane", "") for segment in segments if segment.get("airplane")})
        stop_airports = [
            segment.get("arrival_airport", {}).get("id", "")
            for segment in segments[:-1]
            if segment.get("arrival_airport", {}).get("id")
        ]

        day_name = travel_date.strftime("%A")
        is_weekend = travel_date.weekday() >= 5
        is_holiday = travel_date.isoformat() in INDIA_PUBLIC_HOLIDAYS_2026

        rows.append(
            {
                "scrape_timestamp": scrape_timestamp,
                "search_id": search_id,
                "source_airport": route.source_airport,
                "destination_airport": route.destination_airport,
                "source_city": route.source_city,
                "destination_city": route.destination_city,
                "route": f"{route.source_airport}-{route.destination_airport}",
                "travel_date": travel_date.isoformat(),
                "departure_datetime": departure_dt.isoformat() if departure_dt else "",
                "arrival_datetime": arrival_dt.isoformat() if arrival_dt else "",
                "departure_hour": departure_dt.hour if departure_dt else "",
                "arrival_hour": arrival_dt.hour if arrival_dt else "",
                "airline": " + ".join(airlines),
                "flight_number": " + ".join(flight_numbers),
                "aircraft": " + ".join(aircraft),
                "travel_class": first_segment.get("travel_class", ""),
                "duration_minutes": itinerary.get("total_duration") or first_segment.get("duration", ""),
                "stops": max(len(segments) - 1, 0),
                "stop_airports": "|".join(stop_airports),
                "price": price if price is not None else "",
                "currency": currency,
                "days_left": (travel_date - date.today()).days,
                "day_of_week": day_name,
                "is_weekend": is_weekend,
                "is_holiday": is_holiday,
                "price_category": price_category(price),
                "previous_price": "",
                "price_change": "",
                "price_change_percent": "",
                "surge_flag": False,
                "cheapest_flag": False,
            }
        )

    mark_cheapest(rows)
    return rows


def mark_cheapest(rows: list[dict[str, Any]]) -> None:
    prices = [row["price"] for row in rows if isinstance(row["price"], int)]
    if not prices:
        return
    cheapest = min(prices)
    for row in rows:
        row["cheapest_flag"] = row["price"] == cheapest


def append_csv(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    processed_dir = output_dir / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    csv_path = processed_dir / f"flight_prices_{date.today().isoformat()}.csv"
    write_header = not csv_path.exists()

    with csv_path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    return csv_path


def main() -> None:
    load_dotenv()
    args = parse_args()
    routes = load_routes(Path(args.routes_file))
    dates = travel_dates(args.days_ahead, args.days_count)
    output_dir = Path(args.output_dir)

    api_key = os.getenv("SERPAPI_API_KEY", "")
    if not api_key and not args.dry_run:
        raise SystemExit("Missing SERPAPI_API_KEY. Copy .env.example to .env and add your key.")

    total_rows = 0
    for route in routes:
        for outbound_date in dates:
            params = build_params(route, outbound_date, api_key or "DRY_RUN_KEY", args)
            safe_params = {**params, "api_key": "***"}

            if args.dry_run:
                print(json.dumps(safe_params, indent=2))
                continue

            print(f"Fetching {route.code} for {outbound_date.isoformat()}...")
            payload = fetch_flights(params)
            raw_path = save_raw_json(payload, output_dir, route, outbound_date)
            rows = flatten_payload(payload, route, outbound_date, args.currency)
            csv_path = append_csv(rows, output_dir)
            total_rows += len(rows)
            print(f"Saved {len(rows)} rows | raw={raw_path} | csv={csv_path}")
            time.sleep(args.sleep_seconds)

    if args.dry_run:
        print(f"Dry run complete: {len(routes)} routes x {len(dates)} dates = {len(routes) * len(dates)} requests.")
    else:
        print(f"Done. Flattened rows written: {total_rows}")


if __name__ == "__main__":
    main()
