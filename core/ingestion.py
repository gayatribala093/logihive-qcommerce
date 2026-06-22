# core/ingestion.py
import asyncio
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis

# Import the master module compilation instance from your architecture module
from agents.graph import app_compiled  

class DisruptionAlertPayload(BaseModel):
    alert_id: str = Field(..., description="Unique transaction ID")
    location_zone: str = Field(..., description="Target Mumbai area pocket")
    alert_text: str = Field(..., description="Raw emergency alert string parameters")

class ResumeStatePayload(BaseModel):
    thread_id: str = Field(..., description="Target thread to resume")
    supervisor_token: str = Field(..., description="Administrative security signature token")

redis_client = Redis(host="localhost", port=6379, decode_responses=True)

# =====================================================================
# ⚙️ BACKGROUND TASK WORKER (Thread Pool Loop Isolation)
# =====================================================================
async def slow_path_queue_worker():
    print("🚀 [INIT] LogiHive Background Task Offload Queue Listener initiated.")
    while True:
        try:
            raw_event_wrapper = await redis_client.blpop("queue:slow_path_alerts", timeout=2)
            if not raw_event_wrapper:
                continue
            
            _, event_payload_data = raw_event_wrapper
            parsed_event = json.loads(event_payload_data)
            print(f"📥 [QUEUE WORKER] Captured Alert: {parsed_event.get('alert_text')}. Injecting...")

            thread_config = {"configurable": {"thread_id": str(parsed_event.get("alert_id", "default_id"))}}
            
            # Offload the synchronous module invoke out of the main async loop via thread pools
            await asyncio.to_thread(
                app_compiled.invoke,
                {
                    "active_disruptions": [parsed_event],
                    "live_telemetry_registry": {"current_status": "under_evaluation"}
                },
                config=thread_config
            )
            
        except Exception as queue_err:
            print(f"⚠️ Exception detected inside background worker loop: {str(queue_err)}")
            await asyncio.sleep(1)

@asynccontextmanager
async def app_lifespan_manager(app: FastAPI):
    worker_task = asyncio.create_task(slow_path_queue_worker())
    yield
    worker_task.cancel()
    await redis_client.close()
    print("🛑 Background ingestion workers drained and connection pools closed.")

app = FastAPI(lifespan=app_lifespan_manager)

# =====================================================================
# 📡 NETWORKING ROUTER ENDPOINTS
# =====================================================================
@app.post("/api/telemetry")
async def ingest_telemetry_packet(payload: dict):
    """Fast Write Path: Caches updates into the Redis layer in under 10ms."""
    try:
        await redis_client.set(f"rider:telemetry:active", json.dumps(payload))
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/disruption")
async def ingest_disruption_alert(payload: DisruptionAlertPayload):
    """Slow Path Task Offload: Queues alert items for VLM expansion lookups."""
    try:
        event_str = json.dumps(payload.model_dump())
        await redis_client.rpush("queue:slow_path_alerts", event_str)
        return {"status": "success", "message": "Alert queued safely for graph evaluation."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/agent/resume")
async def resume_agent_workflow(payload: ResumeStatePayload):
    """HITL Webhook Route: Clears state checkpoint breaks and releases threads."""
    if payload.supervisor_token != "LOGIHIVE_SECURE_AUTH_2026":
        raise HTTPException(status_code=403, detail="Invalid supervisor authorization token token.")

    thread_config = {"configurable": {"thread_id": payload.thread_id}}
    try:
        # Prevent database thread pooling race boundaries by calling mutations inside worker threads
        await asyncio.to_thread(
            app_compiled.update_state,
            thread_config,
            {"human_approved": True, "calculated_risk_tensor": 0.0},
            as_node="analyst_agent"
        )
        # Advance graph state execution parameters past the break line
        await asyncio.to_thread(app_compiled.invoke, None, config=thread_config)
        return {"status": "success", "message": f"Thread {payload.thread_id} unblocked safely."}
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Failed to unblock thread context: {str(err)}")