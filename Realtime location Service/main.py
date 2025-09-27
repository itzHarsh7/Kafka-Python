import asyncio, json, logging
from typing import Dict, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer

app = FastAPI()
logging.basicConfig(level=logging.INFO)

BOOTSTRAP = "localhost:9094"
TOPIC   = "driver-positions"
rooms: Dict[str, Set[WebSocket]] = {}

async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode() if k else None,
        value_deserializer=lambda v: v.decode() if v else None
    )
    await consumer.start()
    async for msg in consumer:
        # print("Kafka message:", msg.key, msg.value)
        driver_id = json.loads(msg.key)
        if driver_id in rooms:
            await asyncio.gather(
                *(ws.send_text(msg.value) for ws in rooms[driver_id]),
                return_exceptions=True
            )

@app.on_event("startup")
async def start_consumer():
    asyncio.create_task(consume())

@app.websocket("/ws/live/{driver_id}")
async def ws_endpoint(websocket: WebSocket, driver_id: str):
    await websocket.accept()
    rooms.setdefault(driver_id, set()).add(websocket)
    logging.info("subscribed %s", driver_id)
    try:
        while True:
            await websocket.receive_text()   # keep alive
    except WebSocketDisconnect:
        pass
    finally:
        rooms[driver_id].discard(websocket)
        if not rooms[driver_id]:
            del rooms[driver_id]