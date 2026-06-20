# core/ingestion.py
import asyncio
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis

# Direct architectural link to Gayatri's compiled graph context inside agents/graph.py
from agents.graph import app_compiled  

# =====================================================================
# 📦 HIGH-VELOCITY INGESTION DATA GUARDRAILS (Synced with interfaces.py)
# =====================================================================

class RiderTelemetryPayload(BaseModel):
    rider_id: str = Field(..., description="Unique alphanumeric identifier of the rider")
    latitude: float = Field(..., description="Current GPS latitude coordinate")
    longitude: float = Field(..., description="Current GPS longitude coordinate")
    current_order_id: str = Field(..., description="Active order ID assigned to rider")


class DisruptionAlertPayload(BaseModel):
    alert_id: str = Field(..., description="Unique incident identifier record")
    location_zone: str = Field(..., description="Target Mumbai regional pocket")
    alert_text: str = Field(..., description="Raw textual news snippet or log feed")


class ResumeStatePayload(BaseModel):
    thread_id: str = Field(..., description="Target thread session to unblock")
    supervisor_token: str = Field(..., description="Security access validation token")


# Initialize high-speed ASYNCHRONOUS Redis client interface
redis_client = Redis(host="localhost", port=6379, decode_responses=True)

# =====================================================================
# ⚙️ BACKGROUND ASYNCHRONOUS QUEUE LISTENER (Slow Path Trigger)
# =====================================================================

async def slow_path_queue_worker():
    """
    Continuous asynchronous event queue loop worker pulling unstructured alert incidents
    and programmatically injecting them into the LangGraph automation hive engine.
    """
    print("🚀 [INIT] LogiHive Background Task Offload Queue Listener initiated.")
    while True:
        try:
            # Async non-blocking pop from the dedicated Redis streaming alert ledger
            raw_event_wrapper = await redis_client.blpop("queue:slow_path_alerts", timeout=2)
            if not raw_event_wrapper:
                continue  # Prevents CPU spinning when queues are dry
            
            _, event_payload_data = raw_event_wrapper
            parsed_event = json.loads(event_payload_data)
            print(f"📥 [QUEUE WORKER] Captured Alert: {parsed_event.get('alert_text')}. Injecting into LangGraph...")

            # Seed initial configuration states for the targeted session execution thread
            thread_config = {"configurable": {"thread_id": str(parsed_event.get("alert_id", "default_id"))}}
            
            # ✅ OFF-LOAD TO THREAD WORKER: Safe background execution loop pattern
            # Offloads the synchronous module checkpointer invocation cleanly to keep FastAPI unblocked.
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

# =====================================================================
# 🏢 FASTAPI APPLICATION LIFESPAN CONTEXT MANAGEMENT
# =====================================================================

@asynccontextmanager
async def app_lifespan_manager(app: FastAPI):
    """Resource context manager wrapping application startup and graceful shutdown routines."""
    # Spin up background loop worker safely alongside standard web service tracks
    worker_task = asyncio.create_task(slow_path_queue_worker())
    yield
    # Gracefully cancel tasks and release active network resource blocks during server restarts
    worker_task.cancel()
    await redis_client.close()
    print("🛑 Background ingestion workers drained and connection pools safely closed.")


# Instantiate the final enterprise application gateway server context
app = FastAPI(lifespan=app_lifespan_manager)

# =====================================================================
# 📡 LIVE API ENDPOINTS (Fast Write Path vs Slow Path Offload)
# =====================================================================

@app.post("/api/telemetry")
async def ingest_rider_telemetry(payload: RiderTelemetryPayload):
    """Fast write-path: serializes coordinates into Redis cache under 10ms."""
    try:
        await redis_client.set(
            f"rider:{payload.rider_id}:location", 
            json.dumps(payload.model_dump())
        )
        return {"status": "success", "message": "Telemetry cached successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/disruption")
async def ingest_disruption_alert(payload: DisruptionAlertPayload):
    """Slow-path offload: drops unstructured alerts into the queue for multi-agent expansion."""
    try:
        event_str = json.dumps(payload.model_dump())
        await redis_client.rpush("queue:slow_path_alerts", event_str)
        return {"status": "success", "message": "Disruption alert offloaded to queue worker."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/agent/resume")
async def resume_agent_workflow(payload: ResumeStatePayload):
    """
    Day 5 HITL Gateway Endpoint: Accepts a supervisor validation token, 
    re-loads the state memory matching the thread, clears the breakpoint,
    and pushes the graph forward into the mitigation node calculations.
    """
    # Verify secure administrative access token contract
    if payload.supervisor_token != "LOGIHIVE_SECURE_AUTH_2026":
        raise HTTPException(status_code=403, detail="Invalid supervisor authorization token.")

    # Format the configuration parameter matching our SqliteSaver key requirements
    thread_config = {"configurable": {"thread_id": payload.thread_id}}
    
    try:
        # ✅ RUN IN THREAD POOL WORKER: Pull the current frozen snapshot state from the DB
        current_state_snapshot = await asyncio.to_thread(app_compiled.get_state, thread_config)
        if not current_state_snapshot.next:
            return {"status": "skipped", "message": "Target thread session is completely processed or inactive."}

        print(f"\n🔓 [HITL RELEASE] Webhook Activated for Thread: {payload.thread_id}. Overriding status flags...")

        # ✅ RUN IN THREAD POOL WORKER: Update the state to mark human authorization as valid
        await asyncio.to_thread(
            app_compiled.update_state,
            thread_config,
            {"human_approved": True, "calculated_risk_tensor": 0.0}, # Reset risk parameter levels
            as_node="analyst_agent"
        )

        # ✅ RUN IN THREAD POOL WORKER: Re-invoke execution loop past the breakpoint boundary safely
        await asyncio.to_thread(app_compiled.invoke, None, config=thread_config)
        
        return {
            "status": "success",
            "message": f"Thread {payload.thread_id} unblocked safely. Mitigation instructions committed."
        }
        
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Failed to unblock thread context matrix: {str(err)}")