# Databricks Notebook 01 - Setup and External Delta Tables

## Purpose

Create databases and external Delta table definitions for Bronze, Silver, and Gold layers.

```python
%pip install pyyaml
```

```python
import sys
sys.path.append("/Workspace/Repos/<your-user>/skypulse-flight-analytics")

from src.common.config import load_config

config = load_config("/Workspace/Repos/<your-user>/skypulse-flight-analytics/configs/pipeline_config.yaml")
delta = config["delta"]
```

```sql
CREATE DATABASE IF NOT EXISTS skypulse_bronze;
CREATE DATABASE IF NOT EXISTS skypulse_silver;
CREATE DATABASE IF NOT EXISTS skypulse_gold;
```

```python
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {delta['database_bronze']}.flight_fare_events
USING DELTA
LOCATION '{delta['bronze_path']}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {delta['database_silver']}.flight_fare_events_clean
USING DELTA
LOCATION '{delta['silver_path']}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {delta['database_gold']}.route_price_windows
USING DELTA
LOCATION '{delta['gold_route_window_path']}'
""")
```
