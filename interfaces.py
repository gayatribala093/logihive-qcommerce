# interfaces.py
import os
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Annotated
from concurrent.futures import ThreadPoolExecutor

# Architectural pool isolation logic to keep NetworkX math from hijacking the system
GLOBAL_EXECUTOR = ThreadPoolExecutor(
    max_workers=int(os.getenv("LOGIHIVE_MAX_WORKERS", 4)),
    thread_name_prefix="LogiTwin_Compute_Worker"
)

class RiderTelemetry(BaseModel):
    """Real-time location data packets received from delivery riders."""
    rider_id: str = Field(..., description="Unique identifier of the rider")
    latitude: float = Field(..., description="Current GPS latitude coordinate")
    longitude: float = Field(..., description="Current GPS longitude coordinate")
    current_order_id: str = Field(..., description="Active order ID assigned to rider")

class DarkStoreInventory(BaseModel):
    """Live inventory level monitoring thresholds from local fulfillment nodes."""
    store_id: str = Field(..., description="Unique ID of the neighborhood dark store")
    item_sku: str = Field(..., description="Stock Keeping Unit identifier")
    current_stock_level: int = Field(..., description="Current item count")
    threshold_limit: int = Field(..., description="Safety threshold level before blackout risk")

class DisruptionAlert(BaseModel):
    """Unstructured text anomalies arriving from external APIs."""
    alert_id: str = Field(..., description="Unique identifier for the incoming incident")
    location_zone: str = Field(..., description="Target Mumbai geographical region pocket")
    alert_text: str = Field(..., description="Raw textual news snippet details")