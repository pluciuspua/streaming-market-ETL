from google.cloud import pubsub_v1
from dotenv import load_dotenv
import json, time, requests, os

load_dotenv()

project_id = "live-data-pipeline-471309"
topic_id = "live_data_market"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
SYMBOL = "AAPL"

def fetch_stock_price(symbol):
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY
    }
    r = requests.get(url, params=params).json()
    return {
        "symbol": symbol,
        "price": float(r["Global Quote"]["05. price"]),
        "volume": int(r["Global Quote"]["06. volume"]),
        "timestamp": r["Global Quote"]["07. latest trading day"]
    }

while True:
    msg = fetch_stock_price(SYMBOL)
    data = json.dumps(msg).encode("utf-8")
    publisher.publish(topic_path, data)
    print("Published:", msg)
    time.sleep(60)  # fetch every minute