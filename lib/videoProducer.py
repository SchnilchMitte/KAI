import cv2
import time
from aiokafka import AIOKafkaProducer

class VideoProducer:
    def __init__(self, broker:str, topic:str, service_name:str):
        self.broker = broker
        self.topic = topic
        self.producer = AIOKafkaProducer( 
            bootstrap_servers=self.broker,
            value_serializer=lambda v: v,
            acks= 0,
            compression_type='lz4', #oder zstd ? beide sind schnell GPT sagt testen was besser ist
            batch_size=1024*16, #die anzahl an bytes die angesammelt werden bis eine Batch an Messages zu Kfaka gesendet wird (sollte bei uns 1 frame sein default ist 16kb ???? viel zu klein)
            linger_ms= 5, # Wartezeit befor ein Batch abgesendet wird auch wenn er nicht voll ist ... testen was gut ist 
            retries=0, #verlust sollte uns egal sein denk ich..
            client_id=service_name
        )
        
    async def connect(self):
        try:
            await self.producer.start()
            print('Kafka Producer connected')
        except Exception as e:
            print("Error happened: ",e)
            
    async def send_message(self, data):
        try:
            await self.producer.send_and_wait(self.topic, data)
            print('Message sent successfully')
        except Exception as e:
            print(f"Error sending message: {e}")
            
    async def disconnect(self):
        try:
            await self.producer.stop()
        except Exception as e:
            print("Error while disconnecting", e)
        