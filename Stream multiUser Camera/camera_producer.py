import cv2
import base64
import json
from kafka import KafkaProducer

# Kafka Config
KAFKA_TOPIC = "camera-stream"
KAFKA_SERVER = "localhost:9092"

# Assign unique userId for this producer (camera)
USER_ID = "user123"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cap = cv2.VideoCapture(0)

print(f"📹 Producer started for userId={USER_ID}, streaming video to Kafka...")

frame_count = 0
while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    # Encode frame as JPEG
    _, buffer = cv2.imencode('.jpg', frame)
    jpg_as_text = base64.b64encode(buffer).decode('utf-8')

    # Send message with userId
    message = {"userId": USER_ID, "frame": jpg_as_text}
    producer.send(KAFKA_TOPIC, message)

    frame_count += 1
    if frame_count % 30 == 0:
        print(f"Producer [{USER_ID}] sent {frame_count} frames so far...")

    cv2.imshow(f'Producer - Sending ({USER_ID})', frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()
cv2.destroyAllWindows()
