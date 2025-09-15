# python
# file: publisher_crypto.py
import asyncio
import websockets
import json
from publisher import PubSubPublisher

pub = PubSubPublisher(config_path="C:\\Users\\Lucius\\OneDrive - National University of Singapore\\Desktop\\School\\Tech Projects\\streaming-market-ETL\\config.yaml",
                      topic_id="topic_crypto"
                      )

async def stream_binance():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"
    async with websockets.connect(uri) as websocket:
        while True:
            msg = await websocket.recv()
            event = json.loads(msg)
            try:
                future = pub.publish_json(event)
                message_id = future.result()
                print(f"Published message ID: {message_id}")
            except Exception as e:
                print(f"Error publishing to Pub/Sub: {e}")

if __name__ == "__main__":
    asyncio.run(stream_binance())
