# interfaces.py
import os
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Annotated
from concurrent.futures import ThreadPoolExecutor

# =====================================================================
# 🛡️ ARCHITECTURAL SAFETY BOUNDARY: GLOBAL THREAD POOL EXECUTOR
# =====================================================================
# NetworkX is synchronous and CPU-bound. Kashish MUST wrap mathematical 
# pathing routines inside this executor to avoid freezing FastAPI/LangGraph.
GLOBAL_EXECUTOR = ThreadPoolExecutor(
    max_workers=int(os.getenv("LOGIHIVE_MAX_WORKERS", 4)),
    thread_name_prefix="LogiTwin_Compute_Worker"
)

# =====================================================================
# 📦 HIGH-VELOCITY DATA TRANSFER LAYER CONTRACTS (Pydantic Models)
# =====================================================================

class RiderTelemetry(BaseModel):
    """Real-time location data packets received from delivery riders."""
    rider_id: str = Field(..., description="Unique alphanumeric identifier of the rider")
    latitude: float = Field(..., description="Current GPS latitude coordinate")
    longitude: float = Field(..., description="Current GPS longitude coordinate")
    current_order_id: str = Field(..., description="Active order ID currently assigned to rider")


class DarkStoreInventory(BaseModel):
    """Live inventory level monitoring thresholds from local fulfillment nodes."""
    store_id: str = Field(..., description="Unique ID of the neighborhood dark store leaf node")
    item_sku: str = Field(..., description="Stock Keeping Unit identifier for the item")
    current_stock_level: int = Field(..., description="Current item count in inventory")
    threshold_limit: int = Field(..., description="Safety threshold level before blackout risk")


class DisruptionAlert(BaseModel):
    """Unstructured text anomalies arriving from external news or weather APIs."""
    alert_id: str = Field(..., description="Unique identifier for the incoming incident reporting log")
    location_zone: str = Field(..., description="Target Mumbai geographical region (e.g., Andheri East)")
    alert_text: str = Field(..., description="Raw textual news snippet or weather disruption details")


# =====================================================================
# 🧠 CENTRALIZED STATE MACHINE DATA SPACE (LangGraph State Blueprint)
# =====================================================================

def associative_reducer(current_logs: List[Dict[str, Any]], new_logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deterministic state reducer to eliminate concurrent multi-source overwrite race conditions.
    Appends updates safely into an immutable list timeline instead of standard dictionary updates.
    """
    return current_logs + new_logs


class SystemState(BaseModel):
    """
    The Single Source of Truth Global State Schema passed across LangGraph nodes.
    Uses custom Annotated reducers for appending streaming timeline information safely.
    """
    active_disruptions: List[DisruptionAlert] = Field(default_factory=list)
    calculated_risk_tensor: float = Field(default=0.0, description="Disruption probability scale float between 0.0 and 1.0")
    
    # Telemetry and system updates are protected against non-deterministic overwrite bugs
    telemetry_history: Annotated[List[Dict[str, Any]], associative_reducer] = Field(default_factory=list)
    system_logs: Annotated[List[Dict[str, Any]], associative_reducer] = Field(default_factory=list)
    
    rerouting_plan_proposed: bool = Field(default=False)
    human_approved: bool = Field(default=False)