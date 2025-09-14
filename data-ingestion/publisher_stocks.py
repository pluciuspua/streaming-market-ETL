import time
import requests
import os
from publisher import PubSubPublisher
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
SYMBOL = "AAPL"

pub = PubSubPublisher()  # will use config or env; override with topic_id if needed

def fetch_stock_price(symbol):
    url = "https://www.alphavantage.co/query"
    params = {"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": API_KEY}
    r = requests.get(url, params=params).json()
    return {
        "symbol": symbol,
        "price": float(r["Global Quote"]["05. price"]),
        "volume": int(r["Global Quote"]["06. volume"]),
        "timestamp": r["Global Quote"]["07. latest trading day"]
    }

try:
    while True:
        msg = fetch_stock_price(SYMBOL)
        message_id = pub.publish_json(msg, blocking=True)
        print("Published:", msg, "id:", message_id)
        time.sleep(60)
finally:
    pub.close()