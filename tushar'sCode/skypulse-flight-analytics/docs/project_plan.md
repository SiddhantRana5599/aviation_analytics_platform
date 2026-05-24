# Project Plan

## Step-by-Step Implementation

1. Initialize Git repository and create branch strategy.
2. Download Kaggle Indian flight fare CSV.
3. Validate city coverage and schema.
4. Start Kafka and create `flight_fares_stream`.
5. Run producer to simulate real-time events.
6. Run Bronze stream in Databricks.
7. Run Silver stream with watermarking and deduplication.
8. Run Gold sliding-window aggregation.
9. Build batch Gold feature tables.
10. Train Spark MLlib regression model.
11. Export Gold tables to CSV for Streamlit if direct Databricks access is unavailable.
12. Run dashboard and prepare demo script.

## Expected Output

- Raw events visible in Bronze.
- Clean route-level data visible in Silver.
- Sliding-window fare KPIs visible in Gold.
- Streamlit dashboard with route analytics and predictions.
- ML metrics such as RMSE and R2.

## Troubleshooting

- If Kafka is not reachable from Databricks, demonstrate Spark streaming locally and use Databricks for Delta/Spark SQL.
- If Airflow setup takes too long, show the DAG code and run individual commands manually.
- If ML accuracy is weak, explain feature limitations and focus on pipeline correctness.

