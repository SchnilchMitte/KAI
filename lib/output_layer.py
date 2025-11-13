from dataclasses import dataclass, asdict
from typing import Callable
import json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

class InvalidMetadataError(Exception):
    """Is being thrown if Metadata is missing or corrupt."""
    pass

@dataclass
class OutputLayerMetadata:
    source_name: str
    frame_id: str
    service: str
    timestamp_producer: str
    result: dict

    def to_dict(self):
        return asdict(self)


class OutputLayerProducer:
    def __init__(self, broker: str = "152.53.32.66:9094"):
        self.broker = broker
        self._connected = False
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="lz4",
            acks=0
        )

    async def _connect(self):
        if not self._connected:
            await self.producer.start()
            self._connected = True
            print(f"[SmartInteraction] Connected OutputLayerProducer to {self.broker}")

    def _build_topic(self, metadata: OutputLayerMetadata) -> str:
        if metadata.source_name and metadata.service:
            return f"output.{metadata.source_name}.{metadata.service}"
        raise InvalidMetadataError("Source name and service needs to be declared to build a topic!")

    async def sendMetadata(self, metadata: OutputLayerMetadata):
        """Send serialized metadata to Kafka"""
        #print(metadata.to_dict())
        if not self._connected:
            await self._connect()

        try:
            topic = self._build_topic(metadata)
            data = metadata.to_dict()
            await self.producer.send(topic, data)
            print(f"[SmartInteraction] Sent metadata to topic '{topic}' (frame_id={metadata.frame_id})")
        except Exception as e:
            print(f"[SmartInteraction] Error sending metadata: {e}")

    async def disconnect(self):
        if self._connected:
            await self.producer.stop()
            self._connected = False
            print("[SmartInteraction] Disconnected OutputLayerProducer")


class OutputLayerReceiver:
    def __init__(self, broker: str = "152.53.32.66:9094", group_id: str = None):
        self.broker = broker
        self.group_id = group_id
        self.consumer = None
        self._connected = False

    async def _connect(self, topic: str):
        if self._connected:
            return
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.broker,
            group_id=self.group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        await self.consumer.start()
        self._connected = True
        print(f"[SmartInteraction] Connected OutputLayerReceiver to topic '{topic}'")

    async def receiveMetadata(self, source_name: str, service: str, onMetadata: Callable):
        """Consume metadata messages and call callback"""
        topic = f"output.{source_name}.{service}"
        if not self._connected:
            await self._connect(topic)

        try:
            async for msg in self.consumer:
                print(f"[SmartInteraction] Consumed message from '{msg.topic}' offset {msg.offset}")
                metadata_dict = msg.value
                try:
                    metadata = OutputLayerMetadata(**metadata_dict)
                    if onMetadata:
                        await onMetadata(metadata)
                except Exception as e:
                    print(f"[SmartInteraction] Error parsing metadata: {e}")
        finally:
            await self.consumer.stop()
            self._connected = False
            print(f"[SmartInteraction] Disconnected OutputLayerReceiver from topic '{topic}'")

    async def disconnect(self):
        if self.consumer and self._connected:
            await self.consumer.stop()
            self._connected = False
            print("[SmartInteraction] Receiver disconnected")
