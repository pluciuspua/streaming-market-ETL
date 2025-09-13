# python
import os
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

        try:
            with open(config_path, "r") as f:
                cfg = yaml.safe_load(f) or {}
                project_id = cfg.get("project_id")
                topic_id = cfg.get(topic_id)
        except Exception:
                print("Warning: config.yaml not found or invalid, falling back to env vars only")

        if not project_id or not topic_id:
            raise ValueError("project_id and topic_id must be provided via args, env vars, or config.yaml")

        self._project_id = project_id
        self._topic_id = topic_id
        self._publisher = pubsub_v1.PublisherClient()
        self._topic_path = self._publisher.topic_path(self._project_id, self._topic_id)

    def publish(self, data: Union[bytes, str], blocking: bool = True, attributes: Optional[Dict[str, str]] = None, topic_id: Optional[str] = None) -> Union[str, pubsub_v1.types.PublisherFuture]:
        """
        Publish raw bytes or a string.
        - data: bytes or str (strings are UTF-8 encoded)
        - blocking: if True wait for publish and return message_id, otherwise return Future
        - attributes: optional dict of string attributes
        - topic_id: optional override topic_id for this publish
        """
        if isinstance(data, str):
            data = data.encode("utf-8")
        attrs = attributes or {}

        topic_path = self._topic_path
        if topic_id:
            topic_path = self._publisher.topic_path(self._project_id, topic_id)

        future = self._publisher.publish(topic_path, data, **attrs)
        return future.result() if blocking else future

    def publish_json(self, obj: Any, blocking: bool = True, attributes: Optional[Dict[str, str]] = None, topic_id: Optional[str] = None) -> Union[str, pubsub_v1.types.PublisherFuture]:
        payload = json.dumps(obj, default=str)
        return self.publish(payload, blocking=blocking, attributes=attributes, topic_id=topic_id)

    def publish_bytes(self, data: bytes, blocking: bool = True, attributes: Optional[Dict[str, str]] = None, topic_id: Optional[str] = None) -> Union[str, pubsub_v1.types.PublisherFuture]:
        return self.publish(data, blocking=blocking, attributes=attributes, topic_id=topic_id)

    def close(self):
        try:
            self._publisher.close()
        except Exception:
            pass