import asyncio, websockets, json
from google.cloud import pubsub_v1

# Pub/Sub configuration
project_id = "live-data-pipeline-471309"  # Update with your project ID
topic_id = "live_data_crypto"             # Your topic name
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

async def stream_binance():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"
    async with websockets.connect(uri) as websocket:
        while True:
            msg = await websocket.recv()
            event = json.loads(msg)
            
            # Publish to Pub/Sub
            try:
                # Convert dict back to JSON string for publishing
                data = json.dumps(event).encode("utf-8")
                future = publisher.publish(topic_path, data)
                message_id = future.result()  # Wait for message to be published
                print(f"Published message ID: {message_id}")
            except Exception as e:
                print(f"Error publishing to Pub/Sub: {e}")


if __name__ == "__main__":
    asyncio.run(stream_binance())

# https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#general-wss-information
#'e': 'trade', 'E': 1757173373056, 's': 'BTCUSDT', 't': 5218555707, 'p': '110600.00000000', 'q': '0.00042000', 'T': 1757173373056, 'm': True, 'M': True}
# {
#   "e": "aggTrade",    // Event type
#   "E": 1672515782136, // Event time
#   "s": "BNBBTC",      // Symbol
#   "a": 12345,         // Aggregate trade ID
#   "p": "0.001",       // Price
#   "q": "100",         // Quantity
#   "f": 100,           // First trade ID
#   "l": 105,           // Last trade ID
#   "T": 1672515782136, // Trade time
#   "m": true,          // Is the buyer the market maker?
#   "M": true           // Ignore
# }