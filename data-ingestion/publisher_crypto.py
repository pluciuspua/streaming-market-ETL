# python
# file: publisher_crypto.py
import asyncio
import websockets
import json
from publisher import PubSubPublisher

pub = PubSubPublisher()  # reads env/config or pass topic_id/project_id explicitly

async def stream_binance():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"
    async with websockets.connect(uri) as websocket:
        while True:
            msg = await websocket.recv()
            event = json.loads(msg)

            # publish non-blocking and add callback to log result
            future = pub.publish_json(event, blocking=False)
            def _cb(f):
                try:
                    print("Published message ID:", f.result())
                except Exception as e:
                    print("Publish error:", e)
            future.add_done_callback(_cb)

if __name__ == "__main__":
    try:
        asyncio.run(stream_binance())
    finally:
        pub.close()