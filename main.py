from fastapi import FastAPI
from core.ingestion import router

app = FastAPI(title="Telemetry Service")

app.include_router(router)

@app.get("/")
def home():
    return {"status": "running"}