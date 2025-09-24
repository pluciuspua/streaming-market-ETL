import streamlit as st
from streamlit_autorefresh import st_autorefresh
from visualisations import plot_price_trends, plot_candlestick_chart
from google.cloud import bigquery
import yaml
import time

# Load configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)
    if config:
        PROJECT_ID = config["gcp_project_id"]
        CRYPTO_TABLE = config["bq_table_crypto"]
    else:
        raise Exception("config.yaml is missing")

with open("db_config.yaml", "r") as f:
    db_config = yaml.safe_load(f)
    if db_config:
        CRYPTO_TABLE_QUERY = db_config["crypto_table_query"]
    else:
        raise Exception("db_config.yaml is missing")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Auto-refresh functionality
count = st_autorefresh(interval=300_000, limit=None, key="autorefresh")
st.sidebar.markdown(f"**Refresh Count:** `{count}`")

if st.sidebar.button("🔄 Refresh Now"):
    st.cache_data.clear()
    st.session_state.last_refresh = time.time()
    st.rerun()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_crypto_data():
    crypto_table_query = CRYPTO_TABLE_QUERY.format(CRYPTO_TABLE=CRYPTO_TABLE)
    return client.query(crypto_table_query).to_dataframe()

st.title("📊 Cryptocurrency Market Dashboard")
st.markdown("Real-time price trends and volume analysis with 5-minute aggregated data")

# Load crypto data
df = load_crypto_data()

if not df.empty:
    # Sidebar for symbol selection
    st.sidebar.subheader("📈 Symbol Selection")
    symbols = df["symbol"].unique().tolist()
    selected = st.sidebar.multiselect(
        "Select cryptocurrencies to analyze:",
        symbols,
        default=symbols[:2] if symbols else [],
        help="Choose one or more cryptocurrency symbols to display charts and data"
    )

    # Filter data based on selection
    filtered = df[df["symbol"].isin(selected)] if selected else df

    if selected and not filtered.empty:
        # Summary metrics
        cols = st.columns(len(selected))

        for i, symbol in enumerate(selected):
            symbol_data = filtered[filtered["symbol"] == symbol]
            if not symbol_data.empty:
                latest = symbol_data.iloc[-1]
                previous = symbol_data.iloc[-2] if len(symbol_data) > 1 else latest

                price_change = latest['avg_price'] - previous['avg_price']
                price_change_pct = (price_change / previous['avg_price']) * 100 if previous['avg_price'] != 0 else 0

                with cols[i]:
                    st.metric(
                        label=f"{symbol}",
                        value=f"${latest['avg_price']:.2f}",
                        delta=f"{price_change_pct:+.2f}%"
                    )

        # Price trends section
        st.subheader("📈 Price & Volume Trends")
        for symbol in selected:
            symbol_data = filtered[filtered["symbol"] == symbol]
            if not symbol_data.empty:
                with st.expander(f"📊 {symbol} Price Trends", expanded=True):
                    st.plotly_chart(plot_price_trends(symbol_data, symbol), use_container_width=True)

        # Candlestick charts section
        st.subheader("Candlestick Charts")
        for symbol in selected:
            symbol_data = filtered[filtered["symbol"] == symbol]
            if not symbol_data.empty:
                with st.expander(f"{symbol} Candlestick Chart", expanded=True):
                    st.plotly_chart(plot_candlestick_chart(symbol_data, symbol), use_container_width=True)

    else:
        st.info("📌 Please select one or more cryptocurrency symbols from the sidebar to view charts and data.")

        # Show available symbols
        if symbols:
            st.subheader("Available Cryptocurrencies:")
            symbol_cols = st.columns(min(5, len(symbols)))
            for i, symbol in enumerate(symbols):
                with symbol_cols[i % 5]:
                    st.code(symbol)
else:
    st.error("❌ No cryptocurrency data available. Please check your data pipeline.")
    st.info("🔧 Troubleshooting tips:")
    st.markdown("""
    - Ensure your data pipeline is running
    - Check BigQuery table connectivity
    - Verify configuration files (config.yaml, db_config.yaml)
    - Check if the Binance WebSocket is streaming data
    """)
