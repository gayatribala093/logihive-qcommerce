# simulator.py
import asyncio
import random
import json
import httpx

FASTAPI_GATEWAY_URL = "http://localhost:8000"

async def simulate_streaming_telemetry():
    """Generates continuous rider location transactions to stress-test your fast-path ingestion."""
    print("🛸 Telemetry Ingestion Simulator Active...")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                mock_telemetry_packet = {
                    "rider_id": f"rider_{random.randint(100, 999)}",
                    "latitude": random.uniform(19.010, 19.150),
                    "longitude": random.uniform(72.800, 72.950),
                    "current_order_id": f"ord_{random.randint(5000, 9999)}"
                }
                await client.post(f"{FASTAPI_GATEWAY_URL}/api/telemetry", json=mock_telemetry_packet)
            except Exception:
                pass
            await asyncio.sleep(0.05)  # Continuous 50ms interval data streaming pulses

async def simulate_disruption_triggers():
    """Injects high-risk localized emergencies to test your slow-path multi-agent checkpoint cycles."""
    print("🚨 Emergency Disruption Generation Channel Active...")
    async with httpx.AsyncClient() as client:
        while True:
            await asyncio.sleep(15)  # Introduce an infrastructure storm metric every 15 seconds
            try:
                incident_id = f"mumbai_crisis_{random.randint(1000, 9999)}"
                mock_alert_packet = {
                    "alert_id": incident_id,
                    "location_zone": "Andheri East",
                    "alert_text": "Severe infrastructure blockage reported. Local transportation gridlocked."
                }
                print(f"\n💥 [SIMULATOR ALERT] Dropping Incident Scenario Vector: {incident_id}")
                await client.post(f"{FASTAPI_GATEWAY_URL}/api/disruption", json=mock_alert_packet)
            except Exception as e:
                print(f"Simulator warning: {str(e)}")

async def main():
    await asyncio.gather(simulate_streaming_telemetry(), simulate_disruption_triggers())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Simulation stress matrix closed down successfully.")