# interfaces.py
from pydantic import BaseModel

class RiderTelemetry(BaseModel):
    rider_id: str
    latitude: float
    longitude: float
    current_order_id: str

class DarkStoreInventory(BaseModel):
    store_id: str
    item_sku: str
    current_stock_level: int
    threshold_limit: int

class DisruptionAlert(BaseModel):
    alert_id: str
    location_zone: str
    alert_text: str