
import json
from typing import Optional, Dict, Any, Union
from google.cloud import pubsub_v1
import yaml

class PubSubPublisher:
    """
    Reusable Pub/Sub publisher.
    Usage:
      from data_ingestion.pubsub_publisher import PubSubPublisher
      pub = PubSubPublisher(topic_id="live_data_crypto")
      pub.publish_json({"k":"v"})
      pub.close()
    """

    def __init__(self, config_path: str = "config.yaml", topic_id: Optional[str] = None):
        project_id = None
        config_topic_path = None

        try:
            with open(config_path, "r") as f:
                cfg = yaml.safe_load(f)
                project_id = cfg.get("gcp_project_id")
                config_topic_path = cfg.get(topic_id)
                print(f"config_topic_path: {config_topic_path}")
        except Exception:
            print("Warning: config.yaml not found or invalid, falling back to env vars only")

        if not project_id:
            raise ValueError("project_id and topic_id must be provided via args, env vars, or config.yaml")

        self._project_id = project_id
        self._topic_id = config_topic_path
        self._publisher = pubsub_v1.PublisherClient()
        self._topic_path = f"projects/{self._project_id}/topics/{self._topic_id}"

    def publish_json(self, json_data):
        """
        Publish a JSON-serializable object to Pub/Sub.
        Args:
          json_data: JSON-serializable object (dict, list, etc)
        Returns:
          Future object for the publish operation.
        """
        if not isinstance(json_data, (dict, list)):
            raise ValueError("json_data must be a dict or list")
        data= json.dumps(json_data).encode("utf-8")
        future = self._publisher.publish(self._topic_path, data=data)
        return future
    def close(self):
        """
        Close the Pub/Sub publisher client.
        :return:
        """
        self._publisher.transport.close()