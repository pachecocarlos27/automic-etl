"""Google Cloud Pub/Sub connector for streaming data."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterator
from datetime import datetime
from concurrent.futures import TimeoutError as FuturesTimeoutError

from automic_etl.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


@dataclass
class PubSubConfig:
    """Pub/Sub connection configuration."""

    project_id: str
    topic_id: str | None = None
    subscription_id: str | None = None

    # Authentication
    credentials_path: str | None = None
    credentials_json: str | None = None

    # Subscriber settings
    max_messages: int = 1000
    ack_deadline_seconds: int = 60
    flow_control_max_messages: int = 1000
    flow_control_max_bytes: int = 100 * 1024 * 1024  # 100MB

    # Publisher settings
    batch_max_messages: int = 100
    batch_max_bytes: int = 1024 * 1024  # 1MB
    batch_max_latency: float = 0.01  # 10ms

    # Serialization
    deserializer: str = "json"  # json, string, bytes
    serializer: str = "json"


class PubSubConnector(BaseConnector):
    """
    Google Cloud Pub/Sub connector.

    Supports:
    - Synchronous and asynchronous message consumption
    - Batched publishing with flow control
    - Message ordering with ordering keys
    - Dead letter queues
    - Message filtering

    Example:
        config = PubSubConfig(
            project_id="my-project",
            subscription_id="my-subscription",
        )

        connector = PubSubConnector(config)

        for batch in connector.extract(batch_size=100):
            process_batch(batch)
    """

    def __init__(self, config: PubSubConfig):
        self.config = config
        self._subscriber = None
        self._publisher = None
        self._subscription_path = None
        self._topic_path = None
        self._connected = False

    @property
    def name(self) -> str:
        return f"pubsub:{self.config.project_id}/{self.config.topic_id or self.config.subscription_id}"

    def connect(self) -> None:
        """Establish connection to Pub/Sub."""
        try:
            from google.cloud import pubsub_v1
            from google.oauth2 import service_account

            # Build credentials
            credentials = None
            if self.config.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    self.config.credentials_path
                )
            elif self.config.credentials_json:
                import json as json_lib
                creds_info = json_lib.loads(self.config.credentials_json)
                credentials = service_account.Credentials.from_service_account_info(creds_info)

            # Initialize subscriber
            if self.config.subscription_id:
                self._subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
                self._subscription_path = self._subscriber.subscription_path(
                    self.config.project_id,
                    self.config.subscription_id
                )

            # Initialize publisher
            if self.config.topic_id:
                # Configure batching
                batch_settings = pubsub_v1.types.BatchSettings(
                    max_messages=self.config.batch_max_messages,
                    max_bytes=self.config.batch_max_bytes,
                    max_latency=self.config.batch_max_latency,
                )

                self._publisher = pubsub_v1.PublisherClient(
                    credentials=credentials,
                    batch_settings=batch_settings
                )
                self._topic_path = self._publisher.topic_path(
                    self.config.project_id,
                    self.config.topic_id
                )

            self._connected = True
            logger.info(f"Connected to Pub/Sub project: {self.config.project_id}")

        except ImportError:
            raise ImportError(
                "google-cloud-pubsub is required. "
                "Install with: pip install google-cloud-pubsub"
            )

    def disconnect(self) -> None:
        """Close Pub/Sub connections."""
        if self._subscriber:
            self._subscriber.close()
            self._subscriber = None
        if self._publisher:
            self._publisher = None
        self._connected = False
        logger.info("Disconnected from Pub/Sub")

    def extract(
        self,
        batch_size: int = 100,
        timeout: float = 30.0,
        max_messages: int | None = None,
        transform: Callable[[dict], dict] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Pull messages from Pub/Sub subscription.

        Args:
            batch_size: Number of messages per batch
            timeout: Timeout for pull operation
            max_messages: Maximum total messages to consume
            transform: Optional transformation function

        Yields:
            Batches of deserialized messages
        """
        if not self._connected:
            self.connect()

        if not self._subscriber or not self._subscription_path:
            raise ValueError("Subscription must be configured for extraction")

        total_consumed = 0
        batch = []

        while True:
            if max_messages and total_consumed >= max_messages:
                break

            try:
                # Pull messages
                response = self._subscriber.pull(
                    request={
                        "subscription": self._subscription_path,
                        "max_messages": min(
                            self.config.max_messages,
                            batch_size - len(batch)
                        ),
                    },
                    timeout=timeout,
                )

                if not response.received_messages:
                    # No messages, yield current batch
                    if batch:
                        yield batch
                        batch = []
                    continue

                ack_ids = []

                for received in response.received_messages:
                    message = received.message
                    parsed = self._deserialize_message(message)

                    if transform:
                        parsed = transform(parsed)

                    batch.append(parsed)
                    ack_ids.append(received.ack_id)
                    total_consumed += 1

                    if len(batch) >= batch_size:
                        # Acknowledge messages
                        self._subscriber.acknowledge(
                            request={
                                "subscription": self._subscription_path,
                                "ack_ids": ack_ids,
                            }
                        )
                        yield batch
                        batch = []
                        ack_ids = []

                    if max_messages and total_consumed >= max_messages:
                        break

                # Acknowledge remaining messages
                if ack_ids:
                    self._subscriber.acknowledge(
                        request={
                            "subscription": self._subscription_path,
                            "ack_ids": ack_ids,
                        }
                    )

            except FuturesTimeoutError:
                if batch:
                    yield batch
                    batch = []
            except Exception as e:
                logger.error(f"Error pulling messages: {e}")
                if batch:
                    yield batch
                break

        # Yield remaining messages
        if batch:
            yield batch

    def extract_streaming(
        self,
        callback: Callable[[dict], None],
        timeout: float | None = None,
    ) -> None:
        """
        Stream messages using asynchronous pull.

        Args:
            callback: Function to call for each message
            timeout: How long to stream (None for indefinite)
        """
        if not self._connected:
            self.connect()

        if not self._subscriber or not self._subscription_path:
            raise ValueError("Subscription must be configured for streaming")

        from google.cloud.pubsub_v1.subscriber.message import Message

        def message_callback(message: Message) -> None:
            try:
                parsed = self._deserialize_message(message)
                callback(parsed)
                message.ack()
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                message.nack()

        # Configure flow control
        from google.cloud import pubsub_v1
        flow_control = pubsub_v1.types.FlowControl(
            max_messages=self.config.flow_control_max_messages,
            max_bytes=self.config.flow_control_max_bytes,
        )

        # Start streaming
        streaming_pull_future = self._subscriber.subscribe(
            self._subscription_path,
            callback=message_callback,
            flow_control=flow_control,
        )

        logger.info(f"Started streaming from {self._subscription_path}")

        try:
            streaming_pull_future.result(timeout=timeout)
        except FuturesTimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()

    def _deserialize_message(self, message) -> dict[str, Any]:
        """Deserialize a Pub/Sub message."""
        data = message.data

        if self.config.deserializer == "json":
            value = json.loads(data.decode("utf-8"))
        elif self.config.deserializer == "string":
            value = data.decode("utf-8")
        else:
            value = data

        return {
            "data": value,
            "message_id": message.message_id,
            "publish_time": message.publish_time,
            "attributes": dict(message.attributes),
            "ordering_key": getattr(message, "ordering_key", None),
        }

    def load(
        self,
        data: list[dict[str, Any]],
        attributes: dict[str, str] | None = None,
        ordering_key: str | None = None,
    ) -> dict[str, Any]:
        """
        Publish messages to Pub/Sub topic.

        Args:
            data: List of records to publish
            attributes: Attributes to attach to all messages
            ordering_key: Ordering key for message ordering

        Returns:
            Statistics about the publish operation
        """
        if not self._connected:
            self.connect()

        if not self._publisher or not self._topic_path:
            raise ValueError("Topic must be configured for loading")

        futures = []
        published = 0
        failed = 0

        for item in data:
            try:
                # Serialize data
                if self.config.serializer == "json":
                    serialized = json.dumps(item).encode("utf-8")
                elif self.config.serializer == "string":
                    serialized = str(item).encode("utf-8")
                else:
                    serialized = item if isinstance(item, bytes) else str(item).encode("utf-8")

                # Build message kwargs
                kwargs = {"data": serialized}
                if attributes:
                    kwargs["attributes"] = attributes
                if ordering_key:
                    kwargs["ordering_key"] = ordering_key

                # Publish asynchronously
                future = self._publisher.publish(self._topic_path, **kwargs)
                futures.append(future)

            except Exception as e:
                failed += 1
                logger.error(f"Failed to publish message: {e}")

        # Wait for all publishes to complete
        for future in futures:
            try:
                future.result(timeout=30)
                published += 1
            except Exception as e:
                failed += 1
                logger.error(f"Publish failed: {e}")

        return {
            "published": published,
            "failed": failed,
            "topic": self.config.topic_id,
        }

    def create_topic(self, labels: dict[str, str] | None = None) -> dict[str, Any]:
        """Create a new Pub/Sub topic."""
        if not self._connected:
            self.connect()

        if not self._publisher:
            from google.cloud import pubsub_v1
            self._publisher = pubsub_v1.PublisherClient()

        topic_path = self._publisher.topic_path(
            self.config.project_id,
            self.config.topic_id
        )

        try:
            topic = self._publisher.create_topic(
                request={"name": topic_path, "labels": labels or {}}
            )
            logger.info(f"Created topic: {topic.name}")
            return {"name": topic.name, "labels": dict(topic.labels)}
        except Exception as e:
            logger.error(f"Failed to create topic: {e}")
            raise

    def create_subscription(
        self,
        ack_deadline_seconds: int = 60,
        message_retention_duration: int = 604800,  # 7 days
        filter_expression: str | None = None,
        dead_letter_topic: str | None = None,
        max_delivery_attempts: int = 5,
    ) -> dict[str, Any]:
        """
        Create a new Pub/Sub subscription.

        Args:
            ack_deadline_seconds: Ack deadline
            message_retention_duration: How long to retain unacked messages
            filter_expression: Filter for message attributes
            dead_letter_topic: Topic for dead letter messages
            max_delivery_attempts: Max delivery attempts before dead lettering

        Returns:
            Subscription details
        """
        if not self._connected:
            self.connect()

        if not self._subscriber:
            from google.cloud import pubsub_v1
            self._subscriber = pubsub_v1.SubscriberClient()

        subscription_path = self._subscriber.subscription_path(
            self.config.project_id,
            self.config.subscription_id
        )

        topic_path = f"projects/{self.config.project_id}/topics/{self.config.topic_id}"

        request = {
            "name": subscription_path,
            "topic": topic_path,
            "ack_deadline_seconds": ack_deadline_seconds,
            "message_retention_duration": {"seconds": message_retention_duration},
        }

        if filter_expression:
            request["filter"] = filter_expression

        if dead_letter_topic:
            request["dead_letter_policy"] = {
                "dead_letter_topic": f"projects/{self.config.project_id}/topics/{dead_letter_topic}",
                "max_delivery_attempts": max_delivery_attempts,
            }

        try:
            subscription = self._subscriber.create_subscription(request=request)
            logger.info(f"Created subscription: {subscription.name}")
            return {
                "name": subscription.name,
                "topic": subscription.topic,
                "ack_deadline_seconds": subscription.ack_deadline_seconds,
            }
        except Exception as e:
            logger.error(f"Failed to create subscription: {e}")
            raise

    def get_subscription_info(self) -> dict[str, Any]:
        """Get information about the subscription."""
        if not self._connected:
            self.connect()

        subscription = self._subscriber.get_subscription(
            request={"subscription": self._subscription_path}
        )

        return {
            "name": subscription.name,
            "topic": subscription.topic,
            "ack_deadline_seconds": subscription.ack_deadline_seconds,
            "retain_acked_messages": subscription.retain_acked_messages,
            "message_retention_duration": subscription.message_retention_duration.seconds,
            "labels": dict(subscription.labels),
        }

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
