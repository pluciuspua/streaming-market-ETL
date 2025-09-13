from publisher import PubSubPublisher
from dotenv import load_dotenv
import json, time, requests, os
from datetime import datetime, timedelta

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
SYMBOL = "AAPL"

pub = PubSubPublisher()

def fetch_news_data(symbol):
    now = datetime.now()
    yesterday = now - timedelta(days=1)

    # Format the datetime object into the desired string format
    formatted_time = now.strftime('%Y%m%dT%H%M')
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": symbol,
        "apikey": API_KEY,
        "sort": "RELEVANCE",
        "time_from":yesterday.strftime('%Y%m%dT%H%M'),
        "time_to": now.strftime('%Y%m%dT%H%M')
    }
    r = requests.get(url, params=params).json()
    return r['feed']
try:
    while True:
        msg = fetch_news_data(SYMBOL)
        message_id = pub.publish_json(msg, blocking=True)
        print("Published:", msg, "id:", message_id)
        time.sleep(3600) # fetch every hour
finally:
    pub.close()