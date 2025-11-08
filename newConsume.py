import asyncio
import uuid

from lib import VideoConsumer

async def handle_frame(msg):
    print("Custom callback received:", msg.value)

async def main():
    consumer_id = str(uuid.uuid4()) # für eigene topic group pro consumer 
    consumer = VideoConsumer("localhost:9092", "video-stream", consumer_id)
    await consumer.consume(handle_frame)

asyncio.run(main())
