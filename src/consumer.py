import cv2
import numpy as np
from kafka import KafkaConsumer
import threading
import queue

frame_queues = {
    "video-stream": queue.Queue(maxsize=10),
    "video-stream-file": queue.Queue(maxsize=10)
}

def display_thread(topic):
    """Zeigt Frames für ein Topic im eigenen Fenster."""
    while True:
        frame = frame_queues[topic].get()
        if frame is None:
            break
        frame = cv2.resize(frame, (640, 480))  
        cv2.imshow(f"Stream: {topic}", frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    cv2.destroyWindow(f"Stream: {topic}")

# Threads starten
threads = {}
for topic in frame_queues:
    t = threading.Thread(target=display_thread, args=(topic,), daemon=True)
    t.start()
    threads[topic] = t

# Kafka Consumer: hört auf beide Topics
consumer = KafkaConsumer(
    "video-stream",
    "video-stream-file",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id=None
)

print("Listening for frames from multiple Kafka topics...")

try:
    for msg in consumer:
        topic = msg.topic
        nparr = np.frombuffer(msg.value, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if frame is None:
            continue

        # Nur falls Queue nicht überläuft
        q = frame_queues[topic]
        if not q.full():
            q.put(frame)
        else:
            # Drop frame wenn Queue voll (vermeidet Lag)
            pass

except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    for q in frame_queues.values():
        q.put(None)
    consumer.close()
    cv2.destroyAllWindows()
