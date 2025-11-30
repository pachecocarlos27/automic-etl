"""Data connectors for various sources."""

from automic_etl.connectors.base import BaseConnector, ConnectorConfig
from automic_etl.connectors.registry import ConnectorRegistry, get_connector

# Streaming connectors
from automic_etl.connectors.streaming import (
    KafkaConnector,
    KafkaConfig,
    KinesisConnector,
    KinesisConfig,
    PubSubConnector,
    PubSubConfig,
)

__all__ = [
    # Base
    "BaseConnector",
    "ConnectorConfig",
    "ConnectorRegistry",
    "get_connector",
    # Streaming
    "KafkaConnector",
    "KafkaConfig",
    "KinesisConnector",
    "KinesisConfig",
    "PubSubConnector",
    "PubSubConfig",
]
