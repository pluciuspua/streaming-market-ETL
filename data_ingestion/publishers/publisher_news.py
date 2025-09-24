from dotenv import load_dotenv
import json, time, requests, os
from datetime import datetime, timedelta

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
SYMBOL = "AAPL"
tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]

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
    return [
        {
            "symbol": symbol,
            "headline": article["title"],
            "url": article["url"],
            "title": article["title"],
            "summary": article.get("summary", ""),
            "source": article.get("source", ""),
            "time_published": article.get("time_published", ""),
            "sentiment": article.get("overall_sentiment_label", ""),
            "sentiment_score": article.get("overall_sentiment_score", 0),
            "fetched_at": formatted_time
        }
        for article in r.get("feed", [])
    ]
def stream_news_data(publisher):
    try:
        while True:
            for symbol in tickers:
                msg = fetch_news_data(symbol)
                message_id = publisher.publish_json(msg).result()
                print("Published:", msg, "id:", message_id)
            time.sleep(3600 * 24) # fetch every 24 hours
    finally:
        publisher.close()

if __name__ == "__main__":
    stream_news_data()