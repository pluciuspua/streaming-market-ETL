import streamlit as st

st.title("🏠 Welcome to Market Analytics Dashboard")

st.markdown("""
## Overview
Welcome to your comprehensive financial market analytics platform! This dashboard provides real-time insights into cryptocurrency markets and financial news sentiment analysis.

### 🔍 What You Can Explore:

#### 📊 **Crypto Dashboard**
- Real-time cryptocurrency price trends and volume analysis
- Interactive candlestick charts for technical analysis
- 5-minute aggregated market data
- Multiple cryptocurrency symbols comparison

#### 📰 **News Dashboard** 
- Financial news sentiment analysis using FinBERT AI
- Real-time news feeds from multiple sources
- Sentiment distribution and timeline visualizations
- Filter by cryptocurrency symbols and sentiment types
- Compare AlphaVantage and FinBERT sentiment scores

### 🚀 Getting Started
Use the navigation menu to explore different sections of the dashboard. Data refreshes automatically every 5 minutes, or you can manually refresh using the sidebar controls.

### 📈 Key Features
- **Real-time Data**: Live streaming market data from Binance
- **AI-Powered Sentiment**: Advanced NLP analysis of financial news
- **Interactive Visualizations**: Dynamic charts and graphs
- **Multi-source Analysis**: Combined market and news data insights
- **Responsive Design**: Optimized for desktop and mobile viewing

---
*Data is sourced from Binance WebSocket API and processed through Google Cloud Platform.*
""")

# Add some key metrics or status indicators
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="🔄 Data Update Frequency",
        value="5 minutes",
        delta="Real-time streaming"
    )

with col2:
    st.metric(
        label="🤖 AI Models",
        value="FinBERT",
        delta="Advanced NLP"
    )

with col3:
    st.metric(
        label="📊 Data Sources",
        value="Multiple",
        delta="Binance + News APIs"
    )

st.markdown("---")
st.markdown("💡 **Tip**: Use the sidebar navigation to switch between different dashboard views and customize your data filters.")
