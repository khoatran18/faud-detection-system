import logging
import time
from pathlib import Path

from common.logging.logging_config import setup_logging
from config.settings import load_settings
from stream_layer.ingestion.data_loader import csv_data_generator
from stream_layer.ingestion.kafka_client import KafkaProducer

DATA_TYPE = "test"
DATA_PATH = Path(__file__).parent / "data" / "ingest" / f"{DATA_TYPE}_merged.csv"

def run_producer():

    # Init logging and Kafka producer
    setup_logging()
    logger = logging.getLogger(__name__)
    settings = load_settings()
    kafka_producer = KafkaProducer(settings=settings)
    logger.info("Start ingestion...")

    try:
        topic = settings.kafka.topics.topic
        rows = csv_data_generator(DATA_PATH)

        record_flush_buffer = 0
        total_records = 0
        for idx, record in rows:
            kafka_producer.send(topic, record)
            record_flush_buffer += 1
            total_records += 1

            # Flush buffer if reach max buffer or reach 100 records
            if idx % 100 == 0 or record_flush_buffer >= settings.kafka.producer.max_buffer:
                kafka_producer.flush()
                record_flush_buffer = 0
                time.sleep(1)

            if total_records % 100 == 0:
                logger.info(f"Processed {total_records} records")

    except Exception as e:
        logger.error("Error when ingestion: %s", e)

if __name__ == "__main__":
    run_producer()
