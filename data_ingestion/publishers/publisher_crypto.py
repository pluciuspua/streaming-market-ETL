import asyncio
import websockets
import json


async def stream_binance(publisher):
    uri = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"

    async def heartbeat(websocket):
        while True:
            try:
                await websocket.ping()
                await asyncio.sleep(60)  # Send ping every minute
            except websockets.exceptions.ConnectionClosed:
                break

    async with websockets.connect(uri, ping_interval=60, ping_timeout=10) as websocket:
        # Start heartbeat task
        heartbeat_task = asyncio.create_task(heartbeat(websocket))

        try:
            while True:
                msg = await websocket.recv()
                event = json.loads(msg)
                try:
                    future = publisher.publish_json(event)
                    message_id = future.result()
                    print(f"Published message ID: {message_id}")
                except Exception as e:
                    print(f"Error publishing to Pub/Sub: {e}")
        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")
        finally:
            heartbeat_task.cancel()
