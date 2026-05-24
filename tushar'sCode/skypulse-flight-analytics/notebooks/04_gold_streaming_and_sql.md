# Databricks Notebook 04 - Gold Streaming and Spark SQL

## Purpose

Create sliding-window route analytics and batch Gold tables.

```python
import sys
sys.path.append("/Workspace/Repos/<your-user>/skypulse-flight-analytics")

from src.batch.gold_batch import build_daily_route_metrics, build_ml_features
from src.common.config import load_config
from src.streaming.gold_stream import build_route_price_windows, read_silver_stream, write_route_price_windows

config = load_config("/Workspace/Repos/<your-user>/skypulse-flight-analytics/configs/pipeline_config.yaml")
silver_df = read_silver_stream(spark, config)
window_df = build_route_price_windows(silver_df, config)
query = write_route_price_windows(window_df, config)
```

Sliding window:

```python
window("event_time", "15 minutes", "5 minutes")
```

Batch Gold tables:

```python
build_daily_route_metrics(spark, config)
build_ml_features(spark, config)
```

SQL examples:

```sql
SELECT route, airline, AVG(avg_price) AS avg_fare
FROM skypulse_gold.route_price_windows
GROUP BY route, airline
ORDER BY avg_fare DESC;
```
