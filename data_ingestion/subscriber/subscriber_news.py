from google.cloud import pubsub_v1
import json
import yaml

with open("C:\\Users\\Lucius\\OneDrive - National University of Singapore\\Desktop\\School\\Tech Projects\\streaming-market-ETL\\config.yaml", "r") as f:
    cfg = yaml.safe_load(f)
    project_id = cfg["gcp_project_id"]
    subscription_id = cfg["subscribers"]["news"]

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"Received message: {json.loads(message.data.decode('utf-8'))}")
    message.ack()  # Acknowledge the message


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Subscription interrupted.")
finally:
    subscriber.close()