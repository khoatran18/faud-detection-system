import os
from pathlib import Path
import yaml
import logging
from pydantic import BaseModel

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent

### Kafka Settings
class TopicSettings(BaseModel):
    topic: str

class KafkaServerSettings(BaseModel):
    bootstrap_servers: str

class ProducerSettings(BaseModel):
    retries: int
    max_buffer: int

class KafkaSettings(BaseModel):
    topics: TopicSettings
    server: KafkaServerSettings
    producer: ProducerSettings

### Storage Settings

## Clickhouse Settings
class ClickhouseTableSettings(BaseModel):
    main_table: str
    monitor_table: str

class ClickhouseSettings(BaseModel):
    host: str
    port: int           # HTTP port (8123)
    native_port: int = 9000    # Native TCP port for clickhouse-driver
    username: str
    password: str
    database: str
    jdbc_driver: str
    native_driver: str
    table: ClickhouseTableSettings

## MinIO Settings
class MinioSettings(BaseModel):
    minio_endpoint: str
    minio_endpoint_sdk: str
    minio_access_key: str
    minio_secret_key: str
    core_bucket: str

class StorageSettings(BaseModel):
    clickhouse: ClickhouseSettings
    delta_lake: MinioSettings

## Model Settings
class Model1Settings(BaseModel):
    id: str

class Model2Settings(BaseModel):
    id: str

class ModelSettings(BaseModel):
    model_1: Model1Settings
    model_2: Model2Settings

## Web Dashboard Settings
class WebSettings(BaseModel):
    scan_interval_seconds: int = 10
    host: str = "0.0.0.0"
    port: int = 8000

## Full Settings
class Settings(BaseModel):
    kafka: KafkaSettings
    storage: StorageSettings
    model: ModelSettings
    web: WebSettings = WebSettings()


def load_settings() -> Settings:
    """
    Get APP_ENV to get config file and load
    """

    env = os.getenv("APP_ENV", "dev")
    config_path = BASE_DIR / f"config.{env}.yml"

    logger.info("Loading config with env=%s", env)
    logger.info("Config path: %s", config_path)

    if not config_path.exists():
        logger.error("Config file not found: %s", config_path)
        raise  FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    logger.info("Load config successfully!")

    return Settings(**cfg)


if __name__ == "__main__":
    from common.logging.logging_config import setup_logging
    setup_logging()

    print(load_settings())