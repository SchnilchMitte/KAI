from typing import Callable
from aiokafka import AIOKafkaConsumer
import cv2
import numpy as np

class VideoConsumer:
    def __init__(self, broker:str, topic:str, groupID:str ):
        self.broker= broker
        self.topic= topic
        self._connected= False
        
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
                if onFrame:
                    await onFrame(msg) # Callback jede Gruppe kann selber ihre eigene Callback function schreiben entkoppelt von unserer Logik
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await self.consumer.stop()
    
    async def consume_video(self):
        """Consume frames from Kafka and display video in OpenCV window."""
        if not self._connected:
            await self._connect()

        try:
            async for msg in self.consumer:
                frame_bytes = msg.value
                # Convert bytes to numpy array
                nparr = np.frombuffer(frame_bytes, np.uint8)
                frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if frame is not None:
                    cv2.imshow("Kafka Video Stream", frame)
                    # Press 'q' to exit
                    if cv2.waitKey(1) & 0xFF == ord('q'):
                        break
        finally:
            await self.consumer.stop()
            self._connected = False
            cv2.destroyAllWindows()
            print(f"[Kafka] Disconnected video consumer")
    
    
    async def disconnect(self):
        if self.consumer and self._connected:
            await self.consumer.stop()
            self._connected= False
            print(f"[Kafka] Disconnected")
        else:
            print("Error cannot disconnect no connection was found ")