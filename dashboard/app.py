import streamlit as st

# Configure page settings
st.set_page_config(
    page_title="Market Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

home_page = st.Page("pages/Home.py", title="Home", icon="🏠")
crypto_dashboard_page = st.Page("pages/crypto_dashboard.py", title="Crypto Dashboard", icon="📊")
news_dashboard_page = st.Page("pages/news_dashboard.py", title="News Dashboard", icon="📰")
about_page = st.Page("pages/about.py", title="About", icon="ℹ️")

# Set up navigation
pg = st.navigation({
    "Home": [home_page],
    "Analytics": [crypto_dashboard_page, news_dashboard_page],
    "Info": [about_page]
})

pg.run()
