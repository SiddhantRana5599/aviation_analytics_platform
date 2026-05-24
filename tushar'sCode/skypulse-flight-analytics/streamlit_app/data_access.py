from __future__ import annotations

from pathlib import Path

import pandas as pd


EXPORT_DIR = Path("data/exports")


def read_export(name: str) -> pd.DataFrame:
    path = EXPORT_DIR / name
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def route_window_metrics() -> pd.DataFrame:
    return read_export("route_price_windows.csv")


def route_daily_metrics() -> pd.DataFrame:
    return read_export("route_daily_metrics.csv")


def prediction_samples() -> pd.DataFrame:
    return read_export("prediction_samples.csv")

