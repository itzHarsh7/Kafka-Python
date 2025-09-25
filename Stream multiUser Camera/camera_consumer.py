import cv2
import base64
import json
import numpy as np
from kafka import KafkaConsumer

# Kafka Config
KAFKA_TOPIC = "camera-stream"
KAFKA_SERVER = "localhost:9092"

# Choose which userId’s stream to display
TARGET_USER_ID = "user123"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"🎥 Consumer started, waiting for video frames from userId={TARGET_USER_ID}...")

frame_count = 0
for message in consumer:
    data = message.value

    # Filter by userId
    if data["userId"] != TARGET_USER_ID:
        continue

    frame_data = data['frame']
    decoded = base64.b64decode(frame_data)
    np_frame = np.frombuffer(decoded, dtype=np.uint8)
    frame = cv2.imdecode(np_frame, cv2.IMREAD_COLOR)

    frame_count += 1
    if frame_count % 30 == 0:
        print(f"Consumer received {frame_count} frames from {TARGET_USER_ID}...")

    cv2.imshow(f"Consumer - Receiving ({TARGET_USER_ID})", frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cv2.destroyAllWindows()
