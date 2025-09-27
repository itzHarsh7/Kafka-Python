#!/usr/bin/env python3
import asyncio, aiohttp, json, random, datetime

REST_PROXY = "http://localhost:8082"
TOPIC      = "driver-positions"
DRIVER_ID  = "Driver_001"

async def send_position(session, lat, lon):
    payload = {
        "records": [{
            "key": DRIVER_ID,
            "value": {"lat": lat, "lon": lon, "ts": datetime.datetime.utcnow().isoformat()}
        }]
    }
    async with session.post(f"{REST_PROXY}/topics/{TOPIC}",
                            headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
                            data=json.dumps(payload)) as resp:
        assert resp.status == 200, await resp.text()
        print("sent", payload["records"][0]["value"])

async def main():
    async with aiohttp.ClientSession() as session:
        while True:
            lat = 19.07 + random.uniform(-0.01, 0.01)
            lon = 72.87 + random.uniform(-0.01, 0.01)
            await send_position(session, lat, lon)
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())