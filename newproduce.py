""" import uuid
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

asyncio.run(main()) """


import uuid
import asyncio
from lib import VideoProducer
from lib import FrameGrabber  # make sure FrameGrabber is in lib too

async def main():
    broker = "localhost:9092"
    topic = "video-stream"
    myid = str(uuid.uuid4())

    # Initialize producer and frame grabber
    producer = VideoProducer(broker, topic, myid)
    grabber = FrameGrabber(device=0, width=1920, height=1080, jpeg_quality=80)

    try:
        while True:
            # Capture a frame and send to Kafka
            await producer.send_frame(grabber)
            await asyncio.sleep(0.01)  # ~100 FPS max
    except KeyboardInterrupt:
        print("Stopping video stream...")
    finally:
        grabber.release()
        await producer.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
