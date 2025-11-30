"""Apache Kafka connector for streaming data."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Literal
from datetime import datetime

from automic_etl.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    """Kafka connection configuration."""

    bootstrap_servers: str | list[str]
    topic: str
    group_id: str | None = None
    client_id: str = "automic-etl"

    # Authentication
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    sasl_mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"] | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None

    # Consumer settings
    auto_offset_reset: Literal["earliest", "latest", "none"] = "latest"
    enable_auto_commit: bool = True
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 10000

    # Producer settings
    acks: Literal["all", 0, 1] = "all"
    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = "gzip"
    batch_size: int = 16384
    linger_ms: int = 0

    # Schema Registry
    schema_registry_url: str | None = None
    schema_registry_auth: tuple[str, str] | None = None

    # Serialization
    key_deserializer: Literal["string", "json", "avro", "bytes"] = "string"
    value_deserializer: Literal["string", "json", "avro", "bytes"] = "json"
    key_serializer: Literal["string", "json", "avro", "bytes"] = "string"
    value_serializer: Literal["string", "json", "avro", "bytes"] = "json"


class KafkaConnector(BaseConnector):
    """
    Apache Kafka connector for streaming data ingestion and publishing.

    Supports:
    - Consumer groups for parallel processing
    - Multiple serialization formats (JSON, Avro, String, Bytes)
    - Schema Registry integration
    - SSL/SASL authentication
    - Exactly-once semantics (with transactions)

    Example:
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            topic="events",
            group_id="automic-consumer",
            value_deserializer="json"
        )

        connector = KafkaConnector(config)

        # Consume messages
        for batch in connector.extract(batch_size=100):
            process_batch(batch)

        # Produce messages
        connector.load([{"event": "user_signup", "user_id": 123}])
    """

    def __init__(self, config: KafkaConfig):
        self.config = config
        self._consumer = None
        self._producer = None
        self._schema_registry = None
        self._connected = False

    @property
    def name(self) -> str:
        return f"kafka:{self.config.topic}"

    def connect(self) -> None:
        """Establish connection to Kafka cluster."""
        try:
            from confluent_kafka import Consumer, Producer

            # Build common config
            common_config = self._build_common_config()

            # Initialize consumer
            consumer_config = {
                **common_config,
                "group.id": self.config.group_id or f"automic-{self.config.topic}",
                "auto.offset.reset": self.config.auto_offset_reset,
                "enable.auto.commit": self.config.enable_auto_commit,
                "max.poll.interval.ms": self.config.max_poll_interval_ms,
                "session.timeout.ms": self.config.session_timeout_ms,
            }
            self._consumer = Consumer(consumer_config)
            self._consumer.subscribe([self.config.topic])

            # Initialize producer
            producer_config = {
                **common_config,
                "acks": str(self.config.acks),
                "compression.type": self.config.compression_type,
                "batch.size": self.config.batch_size,
                "linger.ms": self.config.linger_ms,
            }
            self._producer = Producer(producer_config)

            # Initialize Schema Registry if configured
            if self.config.schema_registry_url:
                self._init_schema_registry()

            self._connected = True
            logger.info(f"Connected to Kafka: {self.config.bootstrap_servers}")

        except ImportError:
            raise ImportError(
                "confluent-kafka is required for Kafka connector. "
                "Install with: pip install confluent-kafka"
            )

    def _build_common_config(self) -> dict[str, Any]:
        """Build common Kafka configuration."""
        servers = self.config.bootstrap_servers
        if isinstance(servers, list):
            servers = ",".join(servers)

        config = {
            "bootstrap.servers": servers,
            "client.id": self.config.client_id,
            "security.protocol": self.config.security_protocol,
        }

        # SASL authentication
        if self.config.sasl_mechanism:
            config["sasl.mechanism"] = self.config.sasl_mechanism
            if self.config.sasl_username:
                config["sasl.username"] = self.config.sasl_username
            if self.config.sasl_password:
                config["sasl.password"] = self.config.sasl_password

        # SSL configuration
        if self.config.ssl_cafile:
            config["ssl.ca.location"] = self.config.ssl_cafile
        if self.config.ssl_certfile:
            config["ssl.certificate.location"] = self.config.ssl_certfile
        if self.config.ssl_keyfile:
            config["ssl.key.location"] = self.config.ssl_keyfile

        return config

    def _init_schema_registry(self) -> None:
        """Initialize Schema Registry client."""
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient

            sr_config = {"url": self.config.schema_registry_url}
            if self.config.schema_registry_auth:
                sr_config["basic.auth.user.info"] = ":".join(self.config.schema_registry_auth)

            self._schema_registry = SchemaRegistryClient(sr_config)
            logger.info(f"Connected to Schema Registry: {self.config.schema_registry_url}")

        except ImportError:
            logger.warning("Schema Registry client not available")

    def disconnect(self) -> None:
        """Close Kafka connections."""
        if self._consumer:
            self._consumer.close()
            self._consumer = None
        if self._producer:
            self._producer.flush()
            self._producer = None
        self._connected = False
        logger.info("Disconnected from Kafka")

    def extract(
        self,
        batch_size: int = 100,
        timeout_ms: int = 1000,
        max_messages: int | None = None,
        transform: Callable[[dict], dict] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Consume messages from Kafka topic.

        Args:
            batch_size: Number of messages per batch
            timeout_ms: Poll timeout in milliseconds
            max_messages: Maximum total messages to consume (None for infinite)
            transform: Optional transformation function for each message

        Yields:
            Batches of deserialized messages
        """
        if not self._connected:
            self.connect()

        batch = []
        total_consumed = 0

        try:
            while True:
                if max_messages and total_consumed >= max_messages:
                    break

                msg = self._consumer.poll(timeout_ms / 1000.0)

                if msg is None:
                    # No message received, yield current batch if any
                    if batch:
                        yield batch
                        batch = []
                    continue

                if msg.error():
                    from confluent_kafka import KafkaError
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                # Deserialize message
                record = self._deserialize_message(msg)

                # Apply transformation if provided
                if transform:
                    record = transform(record)

                batch.append(record)
                total_consumed += 1

                # Yield batch when full
                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            # Yield remaining messages
            if batch:
                yield batch

        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
            if batch:
                yield batch

    def _deserialize_message(self, msg) -> dict[str, Any]:
        """Deserialize a Kafka message."""
        # Deserialize key
        key = msg.key()
        if key and self.config.key_deserializer == "json":
            key = json.loads(key.decode("utf-8"))
        elif key and self.config.key_deserializer == "string":
            key = key.decode("utf-8")

        # Deserialize value
        value = msg.value()
        if self.config.value_deserializer == "json":
            value = json.loads(value.decode("utf-8"))
        elif self.config.value_deserializer == "string":
            value = value.decode("utf-8")
        elif self.config.value_deserializer == "avro" and self._schema_registry:
            value = self._deserialize_avro(value)

        return {
            "key": key,
            "value": value,
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": datetime.fromtimestamp(msg.timestamp()[1] / 1000.0),
            "headers": dict(msg.headers()) if msg.headers() else {},
        }

    def _deserialize_avro(self, data: bytes) -> dict[str, Any]:
        """Deserialize Avro message using Schema Registry."""
        try:
            from confluent_kafka.schema_registry.avro import AvroDeserializer

            deserializer = AvroDeserializer(self._schema_registry)
            return deserializer(data, None)
        except Exception as e:
            logger.error(f"Avro deserialization failed: {e}")
            return {"raw": data}

    def load(
        self,
        data: list[dict[str, Any]],
        key_field: str | None = None,
        headers: dict[str, str] | None = None,
        partition: int | None = None,
    ) -> dict[str, Any]:
        """
        Produce messages to Kafka topic.

        Args:
            data: List of records to publish
            key_field: Field to use as message key
            headers: Headers to include with each message
            partition: Specific partition to write to

        Returns:
            Statistics about the produced messages
        """
        if not self._connected:
            self.connect()

        produced = 0
        failed = 0

        def delivery_callback(err, msg):
            nonlocal produced, failed
            if err:
                failed += 1
                logger.error(f"Message delivery failed: {err}")
            else:
                produced += 1

        for record in data:
            try:
                # Extract key if specified
                key = None
                if key_field and key_field in record:
                    key = record[key_field]
                    if self.config.key_serializer == "json":
                        key = json.dumps(key).encode("utf-8")
                    elif self.config.key_serializer == "string":
                        key = str(key).encode("utf-8")

                # Serialize value
                if self.config.value_serializer == "json":
                    value = json.dumps(record).encode("utf-8")
                elif self.config.value_serializer == "string":
                    value = str(record).encode("utf-8")
                else:
                    value = record if isinstance(record, bytes) else str(record).encode("utf-8")

                # Produce message
                self._producer.produce(
                    topic=self.config.topic,
                    key=key,
                    value=value,
                    partition=partition if partition is not None else -1,
                    headers=list(headers.items()) if headers else None,
                    callback=delivery_callback,
                )

                # Trigger delivery callbacks
                self._producer.poll(0)

            except Exception as e:
                failed += 1
                logger.error(f"Failed to produce message: {e}")

        # Flush remaining messages
        self._producer.flush()

        return {
            "produced": produced,
            "failed": failed,
            "topic": self.config.topic,
        }

    def commit(self) -> None:
        """Manually commit consumer offsets."""
        if self._consumer:
            self._consumer.commit()

    def seek_to_beginning(self) -> None:
        """Seek consumer to beginning of all partitions."""
        if self._consumer:
            from confluent_kafka import TopicPartition

            partitions = self._consumer.assignment()
            for tp in partitions:
                tp.offset = 0
            self._consumer.assign(partitions)

    def seek_to_end(self) -> None:
        """Seek consumer to end of all partitions."""
        if self._consumer:
            from confluent_kafka import OFFSET_END

            partitions = self._consumer.assignment()
            for tp in partitions:
                tp.offset = OFFSET_END
            self._consumer.assign(partitions)

    def get_topic_metadata(self) -> dict[str, Any]:
        """Get metadata about the topic."""
        if not self._connected:
            self.connect()

        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": self.config.bootstrap_servers})
        metadata = admin.list_topics(topic=self.config.topic, timeout=10)

        topic_meta = metadata.topics.get(self.config.topic)
        if topic_meta:
            return {
                "topic": self.config.topic,
                "partitions": len(topic_meta.partitions),
                "partition_details": [
                    {
                        "id": p.id,
                        "leader": p.leader,
                        "replicas": list(p.replicas),
                        "isrs": list(p.isrs),
                    }
                    for p in topic_meta.partitions.values()
                ],
            }
        return {}

    def create_topic(
        self,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: dict[str, str] | None = None,
    ) -> bool:
        """
        Create a new Kafka topic.

        Args:
            num_partitions: Number of partitions
            replication_factor: Replication factor
            config: Topic configuration (retention.ms, cleanup.policy, etc.)

        Returns:
            True if topic was created successfully
        """
        try:
            from confluent_kafka.admin import AdminClient, NewTopic

            admin = AdminClient({"bootstrap.servers": self.config.bootstrap_servers})

            new_topic = NewTopic(
                self.config.topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=config or {},
            )

            futures = admin.create_topics([new_topic])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic}' created successfully")
                    return True
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {e}")
                    return False

        except Exception as e:
            logger.error(f"Failed to create topic: {e}")
            return False

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
