# Databricks Notebook 05 - ML Training

## Purpose

Train a Spark MLlib fare prediction model using Gold ML features.

```python
import sys
sys.path.append("/Workspace/Repos/<your-user>/skypulse-flight-analytics")

from src.common.config import load_config
from src.ml.train_price_model import train_price_model

config = load_config("/Workspace/Repos/<your-user>/skypulse-flight-analytics/configs/pipeline_config.yaml")
metrics = train_price_model(spark, config)
metrics
```

Expected output:

```text
{"rmse": <value>, "r2": <value>}
```
