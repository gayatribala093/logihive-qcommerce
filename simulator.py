import asyncio
import httpx
import random
from datetime import datetime

URL = "http://127.0.0.1:8000/ingest"

locations = [
    "Mumbai",
    "Delhi",
    "Pune",
    "Bangalore",
    "Hyderabad"
]

async def send_telemetry(client, shipment_no):

    payload = {
        "shipment_id": f"SHIP{shipment_no}",
        "location": random.choice(locations),
        "temperature": round(random.uniform(20, 40), 2),
        "timestamp": datetime.now().isoformat()
    }

    response = await client.post(URL, json=payload)

    print(
        f"Shipment: {payload['shipment_id']} | "
        f"Status: {response.status_code}"
    )

async def simulator():

    async with httpx.AsyncClient() as client:

        while True:

            tasks = []

            for i in range(100):

                tasks.append(
                    send_telemetry(
                        client,
                        random.randint(1000, 9999)
                    )
                )

            await asyncio.gather(*tasks)

            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(simulator())