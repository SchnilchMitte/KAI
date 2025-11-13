""" 
This is an example workflow - from producing a video using a sensor (e.g your Camera) to processing it and sending
the result to the output layer.
"""
import asyncio
import uuid
import cv2
import numpy as np
from datetime import datetime

from lib import InputLayerProducer
from lib import FrameGrabber
from lib.input_layer import InputLayerConsumer
from lib.output_layer import OutputLayerProducer


######################################################################
# PRODUCER TASK – captures camera frames and publishes to NATS
######################################################################
async def producer_task(topic: str, source_name: str):

    producer = InputLayerProducer(topic=topic, source_name=source_name)
    grabber = FrameGrabber(device=0, width=1920, height=1080, jpeg_quality=40)

    await producer.connect()

    try:
        while True:
            await producer.send_frame(grabber, 30)
    except asyncio.CancelledError:
        print("Producer stopped.")
    finally:
        grabber.release()
        await producer.disconnect()


######################################################################
# CONSUMER TASK – receives frames, processes them, sends metadata
######################################################################

async def consumer_task(topic: str, output_producer: OutputLayerProducer, service_name: str):
   
    consumer = InputLayerConsumer(topic=topic)

    async def fake_processing(frame):
        """YOUR ML STUFF GOES HERE! - 0.01 TO SIMULATE PROCESSING - RETURN YOUR RESULT AS JSON OR DICT!"""
        await asyncio.sleep(0.01)

        return {"status": "ok", "objects": ["car", "person"]}

    async def handle_message(msg):
        """process of nats messages"""
        # JPEG → numpy
        data = np.frombuffer(msg.data, np.uint8)
        frame = cv2.imdecode(data, cv2.IMREAD_COLOR)

        # Show frame
        if frame is not None:
            cv2.imshow("Async Consumer Stream", frame)
            cv2.waitKey(1)

        # ML Processing
        result = await fake_processing(frame)

        # send  Output Layer
        await output_producer.sendMetadata(
            header=msg.headers,
            result=result,
            service_id=service_name
        )


    await consumer.connect()
    await consumer.consume(onFrame=handle_message)


######################################################################
# MAIN WORKFLOW
######################################################################
async def main():
    topic = "cams.cam1"
    service_name = "example_service"

    output_producer = OutputLayerProducer()

    try:
        await asyncio.gather(
            producer_task(topic, "Sensor1"),
            consumer_task(topic, output_producer, service_name)
        )
    except KeyboardInterrupt:
        print("Shutting down workflow...")
    finally:
        await output_producer.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
