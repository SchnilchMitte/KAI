from typing import Callable
from aiokafka import AIOKafkaConsumer

class VideoConsumer:
    def __init__(self, broker:str, topic:str, groupID:str ):
        self.broker= broker
        self.topic= topic
        self.groupID= groupID
        self._connected= None
        
        self.consumer= AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.broker,
            group_id=self.groupID,
            auto_offset_reset="latest",  # start with newest messages
            enable_auto_commit=True,
            value_deserializer=lambda v: v,  # frames already bytes   
        )
        
    async def _connect(self):
        if self._connected:
            return
        await self.consumer.start()
        self._connected= True
        print(f"Connected to Kafka topic '{self.topic}'")
        
    async def consume(self, onFrame: Callable):
        if not self._connected:
            await self._connect()
        try:
            async for msg in self.consumer:
                print("consumed: ", msg.topic, msg.partition, msg.offset, msg.key, msg.value, msg.timestamp)
                await onFrame(msg) # Callback jede Gruppe kann selber ihre eigene Callback function schreiben entkoppelt von unserer Logik
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await self.consumer.stop()
            
    
    async def disconnect(self):
        if self.consumer and self._connected:
            await self.consumer.stop()
            self._connected= False
            print(f"[Kafka] Disconnected")
        else:
            print("Error cannot disconnect no connection was found ")