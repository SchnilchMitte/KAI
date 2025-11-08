""" import asyncio
import uuid

from lib import VideoConsumer

async def handle_frame(msg):
    print("Custom callback received:", msg.value)

async def main():
    consumer_id = str(uuid.uuid4()) # für eigene topic group pro consumer 
    consumer = VideoConsumer("localhost:9092", "video-stream", consumer_id)
    await consumer.consume(handle_frame)

asyncio.run(main())
 """
 
import asyncio
import uuid
import cv2
import numpy as np
from lib import VideoConsumer

async def handle_frame(msg):
    # msg.value is raw JPEG bytes
    frame_data = msg.value
    # Decode JPEG bytes to image
    nparr = np.frombuffer(frame_data, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    if frame is not None:
        cv2.imshow("Video Stream", frame)
        # waitKey 1 is needed to render the frame
        if cv2.waitKey(1) & 0xFF == ord('q'):
            # exit if 'q' is pressed
            raise KeyboardInterrupt
    else:
        print("Failed to decode frame")

async def main():
    consumer_id = str(uuid.uuid4())  # unique group id per consumer
    consumer = VideoConsumer("localhost:9092", "video-stream", consumer_id)
    
    try:
        await consumer.consume(handle_frame)
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        await consumer.disconnect()
        cv2.destroyAllWindows()

if __name__ == "__main__":
    asyncio.run(main())
