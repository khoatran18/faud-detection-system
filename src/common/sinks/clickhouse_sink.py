import logging

from pyspark.sql import SparkSession, DataFrame

from common.logging.logging_config import setup_logging
from config.settings import load_settings, Settings

logger = logging.getLogger(__name__)

class ClickHouseSink:

    def __init__(self, spark: SparkSession):
        setup_logging()
        self.spark = spark
        self.settings: Settings = load_settings()

        self.host = self.settings.storage.clickhouse.host
        self.port = self.settings.storage.clickhouse.port
        self.database = self.settings.storage.clickhouse.database
        self.username = self.settings.storage.clickhouse.username
        self.password = self.settings.storage.clickhouse.password
        self.url = f"jdbc:ch://{self.host}:{self.port}/{self.database}"

    def write_table(self, df: DataFrame, table_name: str):
        """
        Write table to Clickhouse
        """
        logger.info("Start writing to Clickhouse table: %s.%s", self.database, table_name)
        try:
            df.writeTo(f"clickhouse.{self.database}.{table_name}").append()
            logger.info("Finish writing to Clickhouse table: %s.%s", self.database, table_name)
        except Exception as e:
            logger.error("Error when write to Clickhouse table: %s.%s, %s", self.database, table_name, e)