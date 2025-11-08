import uuid
from random import randint
import asyncio

from lib import VideoProducer

async def main():
    broker = "localhost:9092"
    topic = "video-stream"
    myid = str(uuid.uuid4())
    
    producer = VideoProducer(broker, topic, myid)
    
    payload = randint(0, 999)
    while payload != 935:
        await producer.send_message(f"hallo im {myid} and my Payload is {payload}")
        payload = randint(0, 999)  # generate a new random payload each loop
    
    await producer.disconnect()

asyncio.run(main())