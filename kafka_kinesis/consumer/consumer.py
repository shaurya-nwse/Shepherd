from typing import Optional
from aiokafka import AIOKafkaConsumer

from kafka_kinesis.consumer.deserializers import Deserializer


# pylint: disable=too-few-public-methods
class KafkaConsumer:
    """Creates an async Kafka consumer"""

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        group_id: str,
        enable_auto_commit: bool = False,
        auto_offset_reset: str = "earliest",
        value_deserializer: Optional[Deserializer] = None,
        key_deserializer: Optional[Deserializer] = None,
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.value_deserializer = value_deserializer
        self.key_deserializer = key_deserializer
        self.group_id = group_id
        self.enable_auto_commit = enable_auto_commit
        self.auto_offset_reset = auto_offset_reset

    # TODO: Add support for small transformations

    def create_consumer(self) -> AIOKafkaConsumer:
        """Create and return an AIOKafkaConsumer"""
        return AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=(
                self.value_deserializer.deserialize
                if self.value_deserializer
                else lambda x: x
            ),
            key_deserializer=(
                self.key_deserializer.deserialize
                if self.key_deserializer
                else lambda x: x
            ),
            group_id=self.group_id,
            enable_auto_commit=self.enable_auto_commit,
            auto_offset_reset=self.auto_offset_reset,
        )


# pylint: enable=too-few-public-methods
