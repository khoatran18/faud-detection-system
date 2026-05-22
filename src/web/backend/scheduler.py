"""
Background scheduler: polls ClickHouse every SCAN_INTERVAL_SECONDS
and broadcasts the latest stats snapshot to all connected WebSocket clients.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from settings_loader import SCAN_INTERVAL_SECONDS
import clickhouse_client as ch

logger = logging.getLogger(__name__)

# Shared in-memory snapshot — updated by the scheduler, read by API endpoints
_snapshot: dict[str, Any] = {
    "last_updated": None,
    "active_models": [],
    "models": {},
}

# WebSocket connection registry
_ws_connections: set = set()


def get_snapshot(window_mins: int = 30, bucket_str: str = "1 MINUTE") -> dict[str, Any]:
    if window_mins == 30 and bucket_str == "1 MINUTE":
        return _snapshot
    return _fetch_snapshot(window_mins, bucket_str)


def register_ws(ws) -> None:
    _ws_connections.add(ws)


def unregister_ws(ws) -> None:
    _ws_connections.discard(ws)


async def _broadcast(data: dict) -> None:
    import json
    dead = set()
    for ws in _ws_connections:
        try:
            await ws.send_text(json.dumps(data, default=str))
        except Exception:
            dead.add(ws)
    for ws in dead:
        _ws_connections.discard(ws)


def _fetch_snapshot(window_mins: int = 30, bucket_str: str = "1 MINUTE") -> dict[str, Any]:
    """Synchronous ClickHouse fetch — runs in a thread executor."""
    active = ch.get_active_models()
    models: dict[str, Any] = {}
    for mid in active:
        models[mid] = {
            "prediction_stats": ch.get_prediction_stats(mid, window_mins, bucket_str),
            "monitor_stats": ch.get_monitor_stats(mid, window_mins, bucket_str),
        }
    return {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "active_models": active,
        "models": models,
    }


async def run_scheduler() -> None:
    """Infinite loop: fetch → update snapshot → broadcast → sleep."""
    global _snapshot
    logger.info("Scheduler started. Interval: %ss", SCAN_INTERVAL_SECONDS)

    while True:
        try:
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, _fetch_snapshot)
            _snapshot = data
            logger.info(
                "Snapshot updated. Active models: %s", data["active_models"]
            )
            await _broadcast(data)
        except Exception as exc:
            import traceback
            logger.error("Scheduler error: %s\n%s", exc, traceback.format_exc())

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

