# simulator.py
import asyncio
import random
import json
import httpx

FASTAPI_GATEWAY_URL = "http://localhost:8000"

async def simulate_streaming_telemetry():
    """Generates continuous rider location transactions to stress-test your fast-path ingestion."""
    print("🛸 Telemetry Stream Ingestion Simulator Activated...")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                mock_telemetry_packet = {
                    "rider_id": f"rider_{random.randint(100, 999)}",
                    "latitude": random.uniform(19.010, 19.150),
                    "longitude": random.uniform(72.800, 72.950),
                    "current_order_id": f"ord_{random.randint(5000, 9999)}"
                }
                # Stress test the ultra-fast Redis cache ingestion track
                await client.post(f"{FASTAPI_GATEWAY_URL}/api/telemetry", json=mock_telemetry_packet)
            except Exception as e:
                print(f"Simulator Connection Warning (Telemetry Channel): {str(e)}")
            await asyncio.sleep(0.05) # 50ms interval injection density rate

async def simulate_disruption_triggers():
    """Injects high-risk localized emergencies to test your slow-path multi-agent checkpoint cycles."""
    print("🚨 Macroeconomic Anomaly Disruption Channel Activated...")
    async with httpx.AsyncClient() as client:
        while True:
            await asyncio.sleep(12) # Interval window delay before hitting endpoints
            try:
                incident_id = f"mumbai_crisis_{random.randint(1000, 9999)}"
                mock_alert_packet = {
                    "alert_id": incident_id,
                    "location_zone": random.choice(["Andheri East", "Powai", "Bandra West"]),
                    "alert_text": "Severe infrastructure blockage reported. Local transportation gridlocked."
                }
                print(f"\n💥 [SIMULATOR] Injecting Emergency Incident: {incident_id}")
                await client.post(f"{FASTAPI_GATEWAY_URL}/api/disruption", json=mock_alert_packet)
            except Exception as e:
                print(f"Simulator Connection Warning (Alert Channel): {str(e)}")

async def main():
    # Execute both simulation streams concurrently in the asyncio runtime event loop
    await asyncio.gather(
        simulate_streaming_telemetry(),
        simulate_disruption_triggers()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Simulation stress matrix closed down successfully.")