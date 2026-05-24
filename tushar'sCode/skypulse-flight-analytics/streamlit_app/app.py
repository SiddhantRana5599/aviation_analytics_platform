from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from data_access import prediction_samples, route_daily_metrics, route_window_metrics


st.set_page_config(page_title="SkyPulse", page_icon="SP", layout="wide")

st.markdown(
    """
    <style>
    .stApp { background: #101418; color: #f5f7fb; }
    [data-testid="stMetricValue"] { color: #7dd3fc; }
    div[data-testid="stSidebar"] { background: #161b22; }
    </style>
    """,
    unsafe_allow_html=True,
)


def empty_state() -> None:
    st.info("No exported Gold data found yet. Export Databricks Gold tables to data/exports/*.csv for dashboard demo.")


def live_dashboard() -> None:
    st.title("SkyPulse Live Dashboard")
    frame = route_window_metrics()
    if frame.empty:
        empty_state()
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Routes", frame["route"].nunique())
    col2.metric("Airlines", frame["airline"].nunique())
    col3.metric("Avg Fare", f"INR {frame['avg_price'].mean():,.0f}")
    col4.metric("Events", f"{int(frame['fare_event_count'].sum()):,}")

    fig = px.line(
        frame.sort_values("window.start") if "window.start" in frame.columns else frame,
        x="window.start" if "window.start" in frame.columns else frame.index,
        y="avg_price",
        color="route",
        title="Sliding Window Average Fare",
    )
    st.plotly_chart(fig, use_container_width=True)


def route_analytics() -> None:
    st.title("Route Analytics")
    frame = route_daily_metrics()
    if frame.empty:
        empty_state()
        return

    routes = sorted(frame["route"].dropna().unique())
    selected_route = st.selectbox("Route", routes)
    filtered = frame[frame["route"] == selected_route]

    col1, col2 = st.columns(2)
    col1.plotly_chart(px.bar(filtered, x="airline", y="avg_price", title="Average Fare by Airline"), use_container_width=True)
    col2.plotly_chart(px.scatter(filtered, x="avg_duration", y="avg_price", color="airline", title="Duration vs Fare"), use_container_width=True)


def price_prediction() -> None:
    st.title("Price Prediction")
    samples = prediction_samples()
    if samples.empty:
        st.info("Add prediction_samples.csv after running the ML notebook to show model predictions.")
        return

    st.dataframe(samples.head(100), use_container_width=True)
    if {"price", "predicted_price"}.issubset(samples.columns):
        st.plotly_chart(
            px.scatter(samples, x="price", y="predicted_price", color="airline", title="Actual vs Predicted Fare"),
            use_container_width=True,
        )


def streaming_monitor() -> None:
    st.title("Streaming Monitor")
    frame = route_window_metrics()
    if frame.empty:
        empty_state()
        return

    monitor = frame.groupby("route", as_index=False)["fare_event_count"].sum().sort_values("fare_event_count", ascending=False)
    st.plotly_chart(px.bar(monitor, x="route", y="fare_event_count", title="Fare Events Processed by Route"), use_container_width=True)


def historical_trends() -> None:
    st.title("Historical Trends")
    frame = route_daily_metrics()
    if frame.empty:
        empty_state()
        return

    frame["travel_date"] = pd.to_datetime(frame["travel_date"])
    st.plotly_chart(px.line(frame, x="travel_date", y="avg_price", color="route", title="Daily Average Fare"), use_container_width=True)


pages = {
    "Live Dashboard": live_dashboard,
    "Route Analytics": route_analytics,
    "Price Prediction": price_prediction,
    "Streaming Monitor": streaming_monitor,
    "Historical Trends": historical_trends,
}

selected_page = st.sidebar.radio("SkyPulse", list(pages))
pages[selected_page]()
