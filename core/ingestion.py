from fastapi import APIRouter
from pydantic import BaseModel
import redis
import json

router = APIRouter()

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

class TelemetryPayload(BaseModel):
    shipment_id: str
    location: str
    temperature: float
    timestamp: str

@router.post("/ingest")
async def ingest(payload: TelemetryPayload):

    redis_client.rpush(
        "telemetry_events",
        json.dumps(payload.model_dump())
    )

    return {
        "status": "success",
        "message": "Telemetry stored in Redis",
        "data": payload.model_dump()
    }