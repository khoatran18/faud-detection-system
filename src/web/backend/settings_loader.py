"""
Web backend settings loader (settings_loader.py).
Loads from project config/settings.py (reads APP_ENV env var, default=dev).
"""
import sys
import os
from pathlib import Path

# Allow importing from src/ directory
SRC_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SRC_DIR))

from config.settings import load_settings, Settings

_settings: Settings = load_settings()

# ClickHouse connection
CLICKHOUSE_HOST: str = _settings.storage.clickhouse.host
CLICKHOUSE_PORT: int = _settings.storage.clickhouse.native_port  # native TCP port
CLICKHOUSE_USER: str = _settings.storage.clickhouse.username
CLICKHOUSE_PASSWORD: str = _settings.storage.clickhouse.password
CLICKHOUSE_DATABASE: str = _settings.storage.clickhouse.database

# Table names
TABLE_PREDICTIONS: str = _settings.storage.clickhouse.table.main_table
TABLE_MONITOR: str = _settings.storage.clickhouse.table.monitor_table

# Model IDs
MODEL_IDS: list[str] = [
    _settings.model.model_1.id,
    _settings.model.model_2.id,
]

# Web server & scan settings
SCAN_INTERVAL_SECONDS: int = _settings.web.scan_interval_seconds
WEB_HOST: str = _settings.web.host
WEB_PORT: int = _settings.web.port
