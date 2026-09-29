"""
LogiHive Q-Commerce :: simulator.py
Asynchronous client simulating high-frequency rider telemetry updates
and periodic environmental disruption alerts.
"""

import asyncio
import random
import time
import httpx

API_BASE = "http://localhost:8000"

STORES = ["DS_ANDHERI_EAST", "DS_POWAI", "DS_BANDRA", "DS_MULUND", "DS_VASHI"]


async def simulate_telemetry_stream():
    async with httpx.AsyncClient(timeout=5.0) as client:
        print("⚡ [Simulator] Starting high-velocity rider telemetry stream...")
        while True:
            payload = {
                "rider_id": f"RIDER_{random.randint(100, 999)}",
                "latitude": 19.110 + random.uniform(-0.05, 0.05),
                "longitude": 72.850 + random.uniform(-0.05, 0.05),
                "current_order_id": f"ORD_{random.randint(1000, 9999)}",
            }
            try:
                await client.post(f"{API_BASE}/api/telemetry", json=payload)
            except Exception:
                pass
            await asyncio.sleep(0.5)


async def main():
    await simulate_telemetry_stream()


if __name__ == "__main__":
    asyncio.run(main())