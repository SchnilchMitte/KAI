import cv2
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v  
)

topic = "video-stream-file"


video_path = "assets/example_mp4.mp4"

cap = cv2.VideoCapture(video_path)
if not cap.isOpened():
    raise Exception(f"Cannot open video file: {video_path}")

fps = cap.get(cv2.CAP_PROP_FPS)
frame_interval = 1.0 / fps if fps > 0 else 0.03
print(f"Video FPS: {fps}, Frame interval: {frame_interval:.3f}s")

print(f"Streaming '{video_path}' to Kafka topic '{topic}'...")

try:
    while True:
        ret, frame = cap.read()
        if not ret:
            print("End of video reached.")
            break

        
        ret, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        if not ret:
            continue

        producer.send(topic, value=buffer.tobytes())
        time.sleep(frame_interval)
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    cap.release()
    producer.close()
