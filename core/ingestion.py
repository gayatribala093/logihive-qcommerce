# core/ingestion.py
import asyncio
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis

# Direct architectural link to Gayatri's stable graph compilation
from agents.graph import app_compiled  

class DisruptionAlertPayload(BaseModel):
    alert_id: str = Field(..., description="Unique transaction ID")
    location_zone: str = Field(..., description="Target Mumbai area pocket")
    alert_text: str = Field(..., description="Raw emergency alert parameters")

class ResumeStatePayload(BaseModel):
    thread_id: str = Field(..., description="Target thread session to resume")
    supervisor_token: str = Field(..., description="Administrative security token")

redis_client = Redis(host="127.0.0.1", port=6379, decode_responses=True)

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
            
            # ✅ Match: Offload the synchronous graph loop safely to background threads
            await asyncio.to_thread(
                app_compiled.invoke,
                {
                    "active_disruptions": [parsed_event],
                    "live_telemetry_registry": {"current_status": "under_evaluation"}
                },
                config=thread_config
            )
            
        except Exception as queue_err:
            print(f"⚠️ Exception inside background worker loop: {str(queue_err)}")
            await asyncio.sleep(1)

@asynccontextmanager
async def app_lifespan_manager(app: FastAPI):
    worker_task = asyncio.create_task(slow_path_queue_worker())
    yield
    worker_task.cancel()
    await redis_client.close()
    print("🛑 Background ingestion workers drained and pools closed cleanly.")

app = FastAPI(lifespan=app_lifespan_manager)

@app.post("/api/telemetry")
async def ingest_telemetry_packet(payload: dict):
    try:
        await redis_client.set("rider:telemetry:active", json.dumps(payload))
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/disruption")
async def ingest_disruption_alert(payload: DisruptionAlertPayload):
    try:
        event_str = json.dumps(payload.model_dump())
        await redis_client.rpush("queue:slow_path_alerts", event_str)
        return {"status": "success", "message": "Alert queued safely for graph evaluation."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/agent/resume")
async def resume_agent_workflow(payload: ResumeStatePayload):
    if payload.supervisor_token != "LOGIHIVE_SECURE_AUTH_2026":
        raise HTTPException(status_code=403, detail="Invalid supervisor token.")

    thread_config = {"configurable": {"thread_id": payload.thread_id}}
    try:
        # ✅ Match: Update checkpointer rows cleanly inside worker threads
        await asyncio.to_thread(
            app_compiled.update_state,
            thread_config,
            {"human_approved": True, "calculated_risk_tensor": 0.0},
            as_node="analyst_agent"
        )
        # ✅ Match: Advance execution past the frozen state rows synchronously
        await asyncio.to_thread(app_compiled.invoke, None, config=thread_config)
        return {"status": "success", "message": f"Thread {payload.thread_id} unblocked safely."}
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Failed to unblock thread: {str(err)}")