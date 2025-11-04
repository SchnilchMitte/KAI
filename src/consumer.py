""" from kafka import KafkaConsumer

# Connect to Kafka (same host and topic as producer)
consumer = KafkaConsumer(
    "video-stream",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",      # start from earliest messages
    enable_auto_commit=True,
    group_id="test-consumer-group",    # consumer group name
    value_deserializer=lambda v: v.decode("utf-8")  # decode bytes to string
)

print("Listening for messages on 'video-stream'...\n")

try:
    for message in consumer:
        print(f"Received: {message.value}")
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    consumer.close()
     """
     

import cv2
import numpy as np
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "video-stream",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="video-consumers"
)

print("Listening for video frames from Kafka...")

try:
    for msg in consumer:
        # Convert bytes to numpy array
        nparr = np.frombuffer(msg.value, np.uint8)
        # Decode JPEG
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if frame is not None:
            cv2.imshow("Kafka Video", frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
except KeyboardInterrupt:
    print("\nStopped by user")
finally:
    cv2.destroyAllWindows()
    consumer.close()
