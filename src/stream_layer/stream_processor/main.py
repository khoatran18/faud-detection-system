import logging

from common.logging.logging_config import setup_logging
from common.runtime.spark.spark_builder_minio_clickhouse import create_spark_kafka_minio_clickhouse
from common.sources.kafka_source import read_kafka_stream
from config.settings import load_settings
from stream_layer.stream_processor.processor.event_processor import event_processor

def run_stream():
    # Init logging and Kafka producer
    setup_logging()
    logger = logging.getLogger(__name__)
    settings = load_settings()
    logger.info("Start stream processor...")

    try:
        topic = settings.kafka.topics.topic
        spark = create_spark_kafka_minio_clickhouse(app_name="stream_processor", settings=settings).getOrCreate()
        raw_df = read_kafka_stream(spark, settings, topic)
        preprocess_df = event_processor(raw_df)

        preprocess_df.writeStream.format("console").option("truncate", "false") \
            .option("numRows", 100) \
            .start().awaitTermination()

    except Exception as e:
        logger.error("Error when stream processor: %s", e)

if __name__ == "__main__":
    run_stream()