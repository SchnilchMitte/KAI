import asyncio
import uuid

from lib import KAIProducer
from lib import FrameGrabber

async def main():
    
    broker = "152.53.32.66:4222"
    topic = "cams.cam1"
    myid = str(uuid.uuid4())
    
    
    producer = KAIProducer(broker=broker,topic=topic,service_name=myid )
    grabber = FrameGrabber(device=0, width=1920, height=1080, jpeg_quality=40)
    await producer.connect() # you dont need to explicitly connect but its available
    try:
        while True:
            # Capture a frame and send to NATS
            await producer.send_frame(grabber,100) #send_frame will automatically connect you 
    except KeyboardInterrupt:
        print("Stopping video stream...")
    finally:
        grabber.release()
        await producer.disconnect()
    
    
   
    
asyncio.run(main())


