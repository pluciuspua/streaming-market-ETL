import streamlit as st

st.title("About")
st.header("About This Project")
st.markdown("""
This project is a comprehensive data pipeline that ingests, processes, and visualizes financial news sentiment
data. It leverages various technologies including:\n\n

1. Google Cloud Pub/Sub \n\n
2. BigQuery \n\n
3. FinBERT for sentiment analysis\n\n
4. Dataflow - Apache Beam Data Pipeline\n\n
5. Streamlit for web application\n\n
6. Cloud Build for containerization\n\n
7. Cloud Run for deployment\n\n
Finally these insights are presented and visualised in a user-friendly Streamlit web application.
""")

st.image("pages/ETL_archi.png", caption="Architecture Diagram")