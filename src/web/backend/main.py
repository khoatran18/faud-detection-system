"""
FastAPI backend for the Fraud Detection Dashboard.

Start with:
    cd src/web/backend
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
"""
import asyncio
import json
import logging
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware

import clickhouse_client as ch
import scheduler as sched
from settings_loader import (
    MODEL_IDS,
    SCAN_INTERVAL_SECONDS,
    WEB_HOST,
    WEB_PORT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Fraud Detection Dashboard API",
    version="1.0.0",
    description="Backend API for real-time fraud prediction visualization.",
)

# Allow React dev server (port 5173) and any other origin in dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ────────────────────────── Startup / Shutdown ──────────────────────────

@app.on_event("startup")
async def _startup():
    logger.info("Starting background scheduler…")
    asyncio.create_task(sched.run_scheduler())


# ────────────────────────────── REST API ────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/config")
def get_config():
    """Expose current web/scan configuration."""
    return {
        "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
        "known_model_ids": MODEL_IDS,
    }


@app.get("/api/snapshot")
def get_snapshot(window: int = 30, bucket: str = "1 MINUTE"):
    """Return the latest in-memory snapshot (same data as WebSocket push)."""
    return sched.get_snapshot(window, bucket)


@app.post("/api/refresh")
async def force_refresh(window: int = 30, bucket: str = "1 MINUTE"):
    """Immediately trigger a new ClickHouse scan, broadcast, and return result."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, sched._fetch_snapshot, window, bucket)
    if window == 30 and bucket == "1 MINUTE":
        sched._snapshot = data
        await sched._broadcast(data)
    return data



@app.get("/api/models")
def get_models():
    """Return model IDs that currently have data in ClickHouse."""
    return {"active_models": sched.get_snapshot().get("active_models", [])}


@app.get("/api/stats/{model_id}")
def get_stats(model_id: str):
    """Return aggregated prediction + monitor stats for a given model_id."""
    snapshot = sched.get_snapshot()
    model_data = snapshot.get("models", {}).get(model_id)
    if model_data:
        return model_data
    # Fallback: query on demand
    return {
        "prediction_stats": ch.get_prediction_stats(model_id),
        "monitor_stats": ch.get_monitor_stats(model_id),
    }


@app.get("/api/predictions/{model_id}")
def get_predictions(
    model_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """Paginated recent rows from fraud_prediction table."""
    rows = ch.get_recent_predictions(model_id, limit=limit, offset=offset)
    # Convert datetime objects to strings for JSON serialisation
    for r in rows:
        if "process_timestamp" in r and r["process_timestamp"] is not None:
            r["process_timestamp"] = str(r["process_timestamp"])
        if "probability" in r and r["probability"] is not None:
            # Array(Float64) — convert to list
            r["probability"] = list(r["probability"])
    return {"data": rows, "limit": limit, "offset": offset}


@app.get("/api/monitor/{model_id}")
def get_monitor(
    model_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """Paginated recent rows from model_monitor table."""
    rows = ch.get_recent_monitor(model_id, limit=limit, offset=offset)
    for r in rows:
        if "process_timestamp" in r and r["process_timestamp"] is not None:
            r["process_timestamp"] = str(r["process_timestamp"])
    return {"data": rows, "limit": limit, "offset": offset}


# ──────────────────────────── WebSocket ─────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    sched.register_ws(ws)
    logger.info("WebSocket client connected. Total: %d", len(sched._ws_connections))
    try:
        # Send current snapshot immediately on connect
        await ws.send_text(json.dumps(sched.get_snapshot(), default=str))
        while True:
            # Keep connection alive (client sends pings)
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        sched.unregister_ws(ws)
        logger.info("WebSocket client disconnected.")


# ──────────────────────────── Static Files ──────────────────────────────

from fastapi.staticfiles import StaticFiles
import os

frontend_dist = os.path.join(os.path.dirname(__file__), "../frontend/dist")
if os.path.exists(frontend_dist):
    app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="frontend")
    logger.info("Mounted frontend static files from: %s", frontend_dist)


# ──────────────────────────── Entry point ───────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=WEB_HOST, port=WEB_PORT, reload=True)

