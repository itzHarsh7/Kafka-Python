import cv2
import base64
import json
import numpy as np
from kafka import KafkaConsumer

# Kafka Config
KAFKA_TOPIC = "camera-stream"
KAFKA_SERVER = "localhost:9092"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🎥 Consumer started, waiting for video frames from Kafka...")

frame_count = 0
for message in consumer:
    frame_data = message.value['frame']
    decoded = base64.b64decode(frame_data)
    np_frame = np.frombuffer(decoded, dtype=np.uint8)
    frame = cv2.imdecode(np_frame, cv2.IMREAD_COLOR)

    frame_count += 1
    if frame_count % 30 == 0:  # log every 30 frames
        print(f"Consumer received {frame_count} frames from Kafka...")

    cv2.imshow("Consumer - Receiving", frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cv2.destroyAllWindows()
