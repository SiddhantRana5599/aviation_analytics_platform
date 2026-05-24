# Databricks Notebook 03 - Silver Streaming

## Purpose

Clean, standardize, watermark, and deduplicate Bronze events into Silver Delta.

```python
import sys
sys.path.append("/Workspace/Repos/<your-user>/skypulse-flight-analytics")

from src.common.config import load_config
from src.streaming.silver_stream import clean_silver_events, read_bronze_stream, write_silver_stream

config = load_config("/Workspace/Repos/<your-user>/skypulse-flight-analytics/configs/pipeline_config.yaml")
bronze_df = read_bronze_stream(spark, config)
silver_df = clean_silver_events(bronze_df, config)
query = write_silver_stream(silver_df, config)
```

Key concept:

```python
.withWatermark("event_time", "10 minutes")
```
