""" from kafka import KafkaProducer
import time

# Connect to Kafka (adjust if not running locally)
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",  # Kafka advertised listener for host
    value_serializer=lambda v: str(v).encode("utf-8")  # Convert to bytes
)

topic = "video-stream"

print(f" Sending messagse to Kafka topic '{topic}' ...")
count = 0

try:
    while True:
        message = f"Frame {count}"
        producer.send(topic, value=message)
        print(f"Sent: {message}")
        count += 1
        time.sleep(0.01)
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    producer.close()
    
    

 """
 
 
import cv2
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v  # message is already in bytes format 
    #wenn dir ein anderes Format einfällt wie man das ganze performanter senden kann..... 
    #oder mb man könnte komprimierte frames senden mit bissel verlust...
)

topic = "video-stream"
frameWidth = 640
frameHeight = 480

cap = cv2.VideoCapture(0)
if not cap.isOpened():
    raise Exception("Cannot open webcam")
#cap.set(cv2.CAP_PROP_FRAME_WIDTH, frameWidth) cap frames to whatever oyu want
#cap.set(cv2.CAP_PROP_FRAME_HEIGHT, frameHeight) 
width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
fps = cap.get(cv2.CAP_PROP_FPS)  # Might be 0 or approximate depending on camera

print(f"Camera resolution: {width}x{height}, FPS: {fps}")

print("Streaming webcam to Kafka...")

try:
    while True:
        ret, frame = cap.read()
        if not ret:
            continue

        # encode frame as JPEG
        ret, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        if not ret:
            continue

        producer.send(topic, value=buffer.tobytes())
        time.sleep(0.03)  # 0.03 ~30 FPS
except KeyboardInterrupt:
    print("\nStopped by user")
finally:
    cap.release()
    producer.close()
