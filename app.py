import streamlit as st
import pandas as pd
from google.cloud import bigquery
import time



PROJECT_ID = "live-data-pipeline-471309"
DATASET = "crypto_market_data"
TABLE = "crypto_aggregates"

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

st.set_page_config(page_title="Market Dashboard", layout="wide")
st.title("📊 Market Dashboard - Price & Volume (per Minute)")

# Auto-refresh every 60s
refresh_interval = 60
st.caption(f"⏳ Auto-refreshing every {refresh_interval} seconds")

def load_data():
    query = f"""
    SELECT
      symbol,
      TIMESTAMP_TRUNC(window_start, MINUTE) as minute,
      AVG(avg_close) as avg_price,
      SUM(total_volume) as total_volume
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    GROUP BY symbol, minute
    ORDER BY minute ASC
    """
    return client.query(query).to_dataframe()

# Refresh automatically
placeholder = st.empty()
while True:
    df = load_data()

    # Sidebar for symbol selection
    symbols = df["symbol"].unique().tolist()
    selected = st.sidebar.multiselect("Select symbols", symbols, default=symbols[:2])

    filtered = df[df["symbol"].isin(selected)]

    with placeholder.container():
        st.subheader("📈 Avg Price (by Minute)")
        st.line_chart(
            filtered.pivot(index="minute", columns="symbol", values="avg_price")
        )

        st.subheader("📊 Volume Traded (by Minute)")
        st.area_chart(
            filtered.pivot(index="minute", columns="symbol", values="total_volume")
        )

        st.subheader("📄 Raw Data")
        st.dataframe(filtered.tail(50))

    time.sleep(refresh_interval)
