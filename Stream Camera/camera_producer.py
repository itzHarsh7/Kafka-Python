import cv2
import base64
import json
from kafka import KafkaProducer

# Kafka Config
KAFKA_TOPIC = "camera-stream"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# OpenCV camera
cap = cv2.VideoCapture(0)  # 0 for default webcam

print("📹 Producer started, streaming video to Kafka...")

frame_count = 0
while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    # Encode frame as JPEG
    _, buffer = cv2.imencode('.jpg', frame)
    jpg_as_text = base64.b64encode(buffer).decode('utf-8')

    # Send to Kafka
    producer.send(KAFKA_TOPIC, {"frame": jpg_as_text})
    frame_count += 1
    if frame_count % 30 == 0:  # log every 30 frames
        print(f"Producer sent {frame_count} frames so far...")

    # Optional: show locally
    cv2.imshow('Producer - Sending', frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()
cv2.destroyAllWindows()
