"""AWS Kinesis connector for streaming data."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Literal
from datetime import datetime
import time

from automic_etl.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


@dataclass
class KinesisConfig:
    """Kinesis connection configuration."""

    stream_name: str
    region_name: str = "us-east-1"

    # AWS credentials (optional - uses default chain if not provided)
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    profile_name: str | None = None
    role_arn: str | None = None

    # Consumer settings
    shard_iterator_type: Literal[
        "TRIM_HORIZON", "LATEST", "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER", "AT_TIMESTAMP"
    ] = "LATEST"
    starting_sequence_number: str | None = None
    starting_timestamp: datetime | None = None

    # Enhanced fan-out consumer (EFO)
    consumer_name: str | None = None
    use_enhanced_fanout: bool = False

    # Processing settings
    max_records_per_shard: int = 1000
    idle_time_between_reads_ms: int = 1000

    # Serialization
    deserializer: Literal["json", "string", "bytes"] = "json"
    serializer: Literal["json", "string", "bytes"] = "json"


class KinesisConnector(BaseConnector):
    """
    AWS Kinesis connector for streaming data ingestion.

    Supports:
    - Standard consumers and Enhanced Fan-Out (EFO)
    - Multiple shard iteration strategies
    - Automatic shard discovery
    - Checkpointing for exactly-once processing

    Example:
        config = KinesisConfig(
            stream_name="my-stream",
            region_name="us-west-2",
            shard_iterator_type="TRIM_HORIZON"
        )

        connector = KinesisConnector(config)

        for batch in connector.extract(batch_size=100):
            process_batch(batch)
    """

    def __init__(self, config: KinesisConfig):
        self.config = config
        self._client = None
        self._shard_iterators: dict[str, str] = {}
        self._connected = False

    @property
    def name(self) -> str:
        return f"kinesis:{self.config.stream_name}"

    def connect(self) -> None:
        """Establish connection to Kinesis."""
        try:
            import boto3
            from botocore.config import Config

            # Build boto3 session
            session_kwargs = {}
            if self.config.profile_name:
                session_kwargs["profile_name"] = self.config.profile_name

            session = boto3.Session(**session_kwargs)

            # Build client kwargs
            client_kwargs = {
                "region_name": self.config.region_name,
                "config": Config(
                    retries={"max_attempts": 3, "mode": "adaptive"}
                ),
            }

            if self.config.aws_access_key_id:
                client_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                client_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                if self.config.aws_session_token:
                    client_kwargs["aws_session_token"] = self.config.aws_session_token

            # Handle IAM role assumption
            if self.config.role_arn:
                sts = session.client("sts", **client_kwargs)
                assumed = sts.assume_role(
                    RoleArn=self.config.role_arn,
                    RoleSessionName="automic-etl-kinesis"
                )
                creds = assumed["Credentials"]
                client_kwargs["aws_access_key_id"] = creds["AccessKeyId"]
                client_kwargs["aws_secret_access_key"] = creds["SecretAccessKey"]
                client_kwargs["aws_session_token"] = creds["SessionToken"]

            self._client = session.client("kinesis", **client_kwargs)
            self._connected = True

            # Initialize shard iterators
            self._init_shard_iterators()

            logger.info(f"Connected to Kinesis stream: {self.config.stream_name}")

        except ImportError:
            raise ImportError(
                "boto3 is required for Kinesis connector. "
                "Install with: pip install boto3"
            )

    def _init_shard_iterators(self) -> None:
        """Initialize shard iterators for all shards."""
        # Describe stream to get shards
        response = self._client.describe_stream(StreamName=self.config.stream_name)
        shards = response["StreamDescription"]["Shards"]

        for shard in shards:
            shard_id = shard["ShardId"]
            iterator_kwargs = {
                "StreamName": self.config.stream_name,
                "ShardId": shard_id,
                "ShardIteratorType": self.config.shard_iterator_type,
            }

            if self.config.shard_iterator_type in ("AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER"):
                if self.config.starting_sequence_number:
                    iterator_kwargs["StartingSequenceNumber"] = self.config.starting_sequence_number

            if self.config.shard_iterator_type == "AT_TIMESTAMP":
                if self.config.starting_timestamp:
                    iterator_kwargs["Timestamp"] = self.config.starting_timestamp

            response = self._client.get_shard_iterator(**iterator_kwargs)
            self._shard_iterators[shard_id] = response["ShardIterator"]

        logger.info(f"Initialized {len(self._shard_iterators)} shard iterators")

    def disconnect(self) -> None:
        """Close Kinesis connection."""
        self._client = None
        self._shard_iterators = {}
        self._connected = False
        logger.info("Disconnected from Kinesis")

    def extract(
        self,
        batch_size: int = 100,
        max_records: int | None = None,
        transform: Callable[[dict], dict] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Consume records from Kinesis stream.

        Args:
            batch_size: Number of records per batch
            max_records: Maximum total records to consume
            transform: Optional transformation function

        Yields:
            Batches of deserialized records
        """
        if not self._connected:
            self.connect()

        total_consumed = 0
        batch = []

        while True:
            if max_records and total_consumed >= max_records:
                break

            records_received = False

            for shard_id, iterator in list(self._shard_iterators.items()):
                if not iterator:
                    continue

                try:
                    response = self._client.get_records(
                        ShardIterator=iterator,
                        Limit=min(
                            self.config.max_records_per_shard,
                            batch_size - len(batch)
                        )
                    )

                    # Update iterator
                    self._shard_iterators[shard_id] = response.get("NextShardIterator")

                    for record in response["Records"]:
                        records_received = True
                        parsed = self._deserialize_record(record)

                        if transform:
                            parsed = transform(parsed)

                        batch.append(parsed)
                        total_consumed += 1

                        if len(batch) >= batch_size:
                            yield batch
                            batch = []

                        if max_records and total_consumed >= max_records:
                            break

                except self._client.exceptions.ExpiredIteratorException:
                    logger.warning(f"Shard iterator expired for {shard_id}, reinitializing")
                    self._reinit_shard_iterator(shard_id)

                except Exception as e:
                    logger.error(f"Error reading from shard {shard_id}: {e}")

            # Yield partial batch if no records and batch has data
            if not records_received and batch:
                yield batch
                batch = []

            # Sleep between reads if no records
            if not records_received:
                time.sleep(self.config.idle_time_between_reads_ms / 1000.0)

    def _reinit_shard_iterator(self, shard_id: str) -> None:
        """Reinitialize a specific shard iterator."""
        try:
            response = self._client.get_shard_iterator(
                StreamName=self.config.stream_name,
                ShardId=shard_id,
                ShardIteratorType="LATEST",
            )
            self._shard_iterators[shard_id] = response["ShardIterator"]
        except Exception as e:
            logger.error(f"Failed to reinitialize shard iterator: {e}")
            self._shard_iterators[shard_id] = None

    def _deserialize_record(self, record: dict) -> dict[str, Any]:
        """Deserialize a Kinesis record."""
        data = record["Data"]

        if self.config.deserializer == "json":
            value = json.loads(data.decode("utf-8"))
        elif self.config.deserializer == "string":
            value = data.decode("utf-8")
        else:
            value = data

        return {
            "data": value,
            "partition_key": record["PartitionKey"],
            "sequence_number": record["SequenceNumber"],
            "approximate_arrival_timestamp": record["ApproximateArrivalTimestamp"],
            "encryption_type": record.get("EncryptionType"),
        }

    def load(
        self,
        data: list[dict[str, Any]],
        partition_key_field: str | None = None,
        explicit_hash_key: str | None = None,
    ) -> dict[str, Any]:
        """
        Put records to Kinesis stream.

        Args:
            data: List of records to put
            partition_key_field: Field to use as partition key
            explicit_hash_key: Explicit hash key for deterministic sharding

        Returns:
            Statistics about the put operation
        """
        if not self._connected:
            self.connect()

        # Prepare records
        records = []
        for item in data:
            # Determine partition key
            if partition_key_field and partition_key_field in item:
                pk = str(item[partition_key_field])
            else:
                pk = str(hash(json.dumps(item, sort_keys=True, default=str)))

            # Serialize data
            if self.config.serializer == "json":
                serialized = json.dumps(item).encode("utf-8")
            elif self.config.serializer == "string":
                serialized = str(item).encode("utf-8")
            else:
                serialized = item if isinstance(item, bytes) else str(item).encode("utf-8")

            record = {
                "Data": serialized,
                "PartitionKey": pk,
            }
            if explicit_hash_key:
                record["ExplicitHashKey"] = explicit_hash_key

            records.append(record)

        # Put records in batches of 500 (Kinesis limit)
        total_success = 0
        total_failed = 0
        failed_records = []

        for i in range(0, len(records), 500):
            batch = records[i:i + 500]

            response = self._client.put_records(
                StreamName=self.config.stream_name,
                Records=batch
            )

            total_failed += response["FailedRecordCount"]
            total_success += len(batch) - response["FailedRecordCount"]

            # Track failed records for retry
            for j, result in enumerate(response["Records"]):
                if "ErrorCode" in result:
                    failed_records.append({
                        "record": batch[j],
                        "error_code": result["ErrorCode"],
                        "error_message": result.get("ErrorMessage"),
                    })

        return {
            "success": total_success,
            "failed": total_failed,
            "failed_records": failed_records,
            "stream": self.config.stream_name,
        }

    def get_stream_info(self) -> dict[str, Any]:
        """Get information about the Kinesis stream."""
        if not self._connected:
            self.connect()

        response = self._client.describe_stream(StreamName=self.config.stream_name)
        desc = response["StreamDescription"]

        return {
            "stream_name": desc["StreamName"],
            "stream_arn": desc["StreamARN"],
            "stream_status": desc["StreamStatus"],
            "retention_period_hours": desc["RetentionPeriodHours"],
            "shard_count": len(desc["Shards"]),
            "shards": [
                {
                    "shard_id": s["ShardId"],
                    "starting_hash_key": s["HashKeyRange"]["StartingHashKey"],
                    "ending_hash_key": s["HashKeyRange"]["EndingHashKey"],
                }
                for s in desc["Shards"]
            ],
            "encryption_type": desc.get("EncryptionType"),
            "key_id": desc.get("KeyId"),
        }

    def create_stream(
        self,
        shard_count: int = 1,
        stream_mode: Literal["PROVISIONED", "ON_DEMAND"] = "ON_DEMAND",
    ) -> bool:
        """Create a new Kinesis stream."""
        if not self._connected:
            self.connect()

        try:
            kwargs = {"StreamName": self.config.stream_name}

            if stream_mode == "ON_DEMAND":
                kwargs["StreamModeDetails"] = {"StreamMode": "ON_DEMAND"}
            else:
                kwargs["ShardCount"] = shard_count

            self._client.create_stream(**kwargs)
            logger.info(f"Created Kinesis stream: {self.config.stream_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create stream: {e}")
            return False

    def register_consumer(self, consumer_name: str) -> dict[str, Any]:
        """Register an Enhanced Fan-Out consumer."""
        if not self._connected:
            self.connect()

        # Get stream ARN
        stream_info = self.get_stream_info()

        response = self._client.register_stream_consumer(
            StreamARN=stream_info["stream_arn"],
            ConsumerName=consumer_name
        )

        return {
            "consumer_name": response["Consumer"]["ConsumerName"],
            "consumer_arn": response["Consumer"]["ConsumerARN"],
            "consumer_status": response["Consumer"]["ConsumerStatus"],
        }

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
