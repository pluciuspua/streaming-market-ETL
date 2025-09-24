import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh
from visualisations import plot_sentiment_distribution, plot_sentiment_timeline, plot_sentiment_by_symbol, create_news_summary_cards
from google.cloud import bigquery
import yaml
import time

# Load configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)
    if config:
        PROJECT_ID = config["gcp_project_id"]
        NEWS_TABLE = config["bq_table_news"]
    else:
        raise Exception("config.yaml is missing")

with open("db_config.yaml", "r") as f:
    db_config = yaml.safe_load(f)
    if db_config:
        NEWS_TABLE_QUERY = db_config["news_table_query"]
    else:
        raise Exception("db_config.yaml is missing")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Auto-refresh functionality
count = st_autorefresh(interval=300_000, limit=None, key="autorefresh")

if st.sidebar.button("🔄 Refresh Now"):
    st.cache_data.clear()
    st.session_state.last_refresh = time.time()
    st.rerun()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_news_data():
    news_table_query = NEWS_TABLE_QUERY.format(NEWS_TABLE=NEWS_TABLE)
    query_news_df = client.query(news_table_query).to_dataframe()
    query_news_df["published_at"] = pd.to_datetime(query_news_df["published_at"])
    return query_news_df

st.title("📰 News & Sentiment Analysis Dashboard")
st.markdown("AI-powered financial news sentiment analysis using FinBERT and AlphaVantage")

# Load news data + apply filters if neeeded
news_df = load_news_data()

if not news_df.empty:
    # Sidebar filters for news
    st.sidebar.subheader("🔍 Filters")

    # Symbol filter
    news_symbols = news_df["symbol"].unique().tolist()
    selected_news_symbols = st.sidebar.multiselect(
        "Select cryptocurrencies:",
        news_symbols,
        default=news_symbols[:3] if len(news_symbols) >= 3 else news_symbols,
        help="Filter news by cryptocurrency symbols"
    )
    # Time range filter
    time_range = st.sidebar.selectbox(
        "Time range:",
        ["Last 24 hours", "Last 12 hours", "Last 6 hours", "Last 3 hours"],
        help="Select the time window for news analysis"
    )

    # Apply filters
    filtered_news = news_df.copy()

    if selected_news_symbols:
        filtered_news = filtered_news[filtered_news["symbol"].isin(selected_news_symbols)]

    # Apply time filter
    hours_map = {
        "Last 24 hours": 24,
        "Last 12 hours": 12,
        "Last 6 hours": 6,
        "Last 3 hours": 3
    }

    hours_back = hours_map[time_range]
    cutoff_time = pd.Timestamp.now() - pd.Timedelta(hours=hours_back)
    cutoff_time = cutoff_time.tz_localize("UTC")
    filtered_news = filtered_news[filtered_news["published_at"] >= cutoff_time]


    # SUMMARY CARD SEGMENT
    summary_stats = create_news_summary_cards(filtered_news)

    st.subheader("📊 News Analytics Overview")
    st.markdown("")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Articles", summary_stats['total_articles'])
    with col2:
        sentiment = summary_stats['most_commmon_finbert_sentiment']

        # Define color mapping
        color_mapping = {
            'positive': '#00ff00',
            'negative': '#ff0000',
            'neutral': '#ffa500'
        }

        color = color_mapping.get(sentiment.lower(), '#gray')

        st.markdown(
            f"**Dominant FinBERT Sentiment**  \n<span style='color:{color}; font-size:24px; font-weight:bold'>{sentiment.title()}</span>",
            unsafe_allow_html=True
        )
    with col3:
        latest_time = summary_stats['latest_article_time']
        formatted_time = pd.to_datetime(latest_time).strftime("%H:%M") if latest_time != 'N/A' else 'N/A'
        st.metric("🕐 Latest Article", formatted_time)

    st.subheader("Alphavantage Sentiment Overview")
    c1, c2 = st.columns(2)
    with c1:
        av_sentiment = summary_stats['most_common_alphavantage_sentiment']

        # Map AlphaVantage sentiment to colors
        av_color_mapping = {
            'bullish': '#00ff00',
            'somewhat-bullish': '#90EE90',
            'neutral': '#ffa500',
            'somewhat-bearish': '#ffcccb',
            'bearish': '#ff0000'
        }

        color = av_color_mapping.get(av_sentiment.lower(), '#gray')

        st.markdown(
            f"**Dominant AlphaVantage Sentiment**  \n<span style='color:{color}; font-size:24px; font-weight:bold'>{av_sentiment.title()}</span>",
            unsafe_allow_html=True
        )
    with c2:
        st.metric("Avg Alphavantage Score", f"{summary_stats['avg_alphavantage_score']:.3f}")
    st.markdown("---")
    st.markdown("**Alphavantage Sentiment Score Interpretation:**",)
    st.markdown('''x <= -0.35: Bearish \n\n  -0.35 < x <= -0.15: Somewhat-Bearish\n\n  -0.15 < x < 0.15 Neutral\n\n  0.15 <= x < 0.35: Somewhat-Bullish\n\n  x >= 0.35: Bullish ''')

    # sentiment visulization comparison btw finbert and alphavantage
    st.subheader("📈 Sentiment Visualizations")

    col1, col2 = st.columns(2)

    with col1:
        sentiment_pie = plot_sentiment_distribution(filtered_news, "finbert_sentiment")
        if sentiment_pie:
            st.plotly_chart(sentiment_pie, use_container_width=True)
        else:
            st.info("No sentiment data available for pie chart")

    with col2:
        sentiment_by_symbol = plot_sentiment_by_symbol(filtered_news, "finbert_sentiment")
        if sentiment_by_symbol:
            st.plotly_chart(sentiment_by_symbol, use_container_width=True)
        else:
            st.info("No data available for sentiment by symbol chart")

    c1, c2 = st.columns(2)
    with c1:
        av_sentiment_pie = plot_sentiment_distribution(filtered_news, "alphavantage_sentiment")
        if av_sentiment_pie:
            st.plotly_chart(av_sentiment_pie, use_container_width=True)
        else:
            st.info("No Alphavantage sentiment data available for pie chart")
    with c2:
        av_sentiment_by_symbol = plot_sentiment_by_symbol(filtered_news, "alphavantage_sentiment")
        if av_sentiment_by_symbol:
            st.plotly_chart(av_sentiment_by_symbol, use_container_width=True)
        else:
            st.info("No data available for Alphavantage sentiment by symbol chart")

    # Timeline chart
    st.subheader("📈 Sentiment Timeline Analysis")
    sentiment_timeline_finbert = plot_sentiment_timeline(filtered_news, "finbert_sentiment")
    if sentiment_timeline_finbert:
        st.plotly_chart(sentiment_timeline_finbert, use_container_width=True)
    else:
        st.info("No data available for FinBERT sentiment timeline")

    av_sentiment_timeline = plot_sentiment_timeline(filtered_news, "alphavantage_sentiment")
    if av_sentiment_timeline:
        st.plotly_chart(av_sentiment_timeline, use_container_width=True)
    else:
        st.info("No data available for Alphavantage sentiment timeline")

    # News articles section
    st.subheader("📰 Recent News Articles")

    # Article display options
    col1, col2 = st.columns(2)
    with col1:
        articles_to_show = st.selectbox("Number of articles to display:", [10, 20, 50], index=1)
    with col2:
        sort_by = st.selectbox("Sort by:", ["Most Recent", "Highest Sentiment Score", "Lowest Sentiment Score"])

    # Sort articles based on selection
    display_news = filtered_news.copy()
    if sort_by == "Most Recent":
        display_news = display_news.sort_values('published_at', ascending=False)
    elif sort_by == "Highest Sentiment Score":
        display_news = display_news.sort_values('finbert_score', ascending=False, na_position='last')
    else:  # Lowest Sentiment Score
        display_news = display_news.sort_values('finbert_score', ascending=True, na_position='last')

    # Display news in an expandable format
    for idx, row in display_news.head(articles_to_show).iterrows():
        title = row['title']
        short_title = f"{title[:80]}..." if len(title) > 80 else title

        with st.expander(f"🗞️ {short_title}"):
            col1, col2, col3 = st.columns([2, 1, 1])

            with col1:
                st.write("**📝 Description:**")
                description = str(row['description'])
                short_desc = f"{description[:300]}..." if len(description) > 300 else description
                st.write(short_desc)
                st.write(f"**📰 Source:** {row['source']}")
                if row['url']:
                    st.write(f"**🔗 URL:** [{row['url']}]({row['url']})")

            with col2:
                st.write("**🤖 FinBERT Analysis:**")
                sentiment = str(row['finbert_sentiment']).lower()
                sentiment_color = {
                    'positive': 'green',
                    'negative': 'red',
                    'neutral': 'orange'
                }.get(sentiment, 'gray')

                st.markdown(f"Sentiment: <span style='color:{sentiment_color}'><b>{sentiment.title()}</b></span>", unsafe_allow_html=True)
                st.write(f"Score: {row['finbert_score']:.3f}")

            with col3:
                st.write("**📊 AlphaVantage:**")
                av_sentiment = str(row['alphavantage_sentiment']).lower()
                av_sentiment_color = {
                    'positive': 'green',
                    'negative': 'red',
                    'neutral': 'orange',
                    'somewhat-bullish': 'lightgreen',
                    'somewhat-bearish': 'lightcoral'
                }.get(av_sentiment, 'gray')

                st.markdown(f"Sentiment: <span style='color:{av_sentiment_color}'><b>{av_sentiment.title()}</b></span>", unsafe_allow_html=True)
                st.write(f"Score: {row['alphavantage_sentiment_score']:.3f}")

            # Additional info in a separate row
            st.write(f"**📅 Published:** {pd.to_datetime(row['published_at']).strftime('%Y-%m-%d %H:%M:%S')}")
            st.write(f"**💰 Symbol:** {row['symbol']}")

else:
    st.error("❌ No news data available. Please check your data pipeline.")
    st.info("🔧 Troubleshooting tips:")
    st.markdown("""
    - Ensure your news data pipeline is running
    - Check BigQuery table connectivity for news data
    - Verify AlphaVantage API is functioning
    - Check if FinBERT API is accessible
    - Verify configuration files (config.yaml, db_config.yaml)
    """)
