"""
LogiHive Q-Commerce :: interfaces.py
Central data contracts and thread pool execution boundary.
"""

from __future__ import annotations

import concurrent.futures
from typing import List, Optional, Any
from pydantic import BaseModel, Field

# Bounded global ThreadPoolExecutor to isolate CPU-bound NetworkX graph computations
EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)


class RiderTelemetry(BaseModel):
    rider_id: str
    latitude: float
    longitude: float
    current_order_id: str


class DarkStoreInventory(BaseModel):
    store_id: str
    item_sku: str
    current_stock_level: int
    threshold_limit: int = 2000


class DisruptionAlert(BaseModel):
    alert_id: str
    location_zone: Optional[str] = None
    alert_text: Optional[str] = None
    category: Optional[str] = "weather"
    affected_area: Optional[str] = None
    raw_text: Optional[str] = None
    reported_at: Optional[float] = None
    severity_hint: Optional[str] = "high"

    def get_zone(self) -> str:
        return self.affected_area or self.location_zone or "DS_ANDHERI_EAST"

    def get_text(self) -> str:
        return self.raw_text or self.alert_text or "Severe localized waterlogging reported."