import logging

from pyspark.sql.functions import lit, col, from_json
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, BooleanType, TimestampType, StringType

from common.logging.logging_config import setup_logging
from common.runtime.clickhouse.clickhouse_init import init_clickhouse
from common.runtime.clickhouse.prepare_clickhouse_native import prepare_clickhouse_native
from common.runtime.spark.spark_builder_minio_clickhouse import create_spark_kafka_minio_clickhouse
from common.sinks.clickhouse_sink import ClickHouseSink
from common.sources.kafka_source import read_kafka_stream
from config.settings import load_settings

MONITOR_SCHEMA = StructType([
    StructField("TransactionID", LongType(), True),
    StructField("model_id", StringType(), True),
    StructField("model_predict", IntegerType(), True),
    StructField("actual_result", IntegerType(), True),
    StructField("is_correct", IntegerType(), True),
    StructField("process_timestamp", TimestampType(), True),
])

def run_stream():
    # Init logging and Kafka producer
    setup_logging()
    logger = logging.getLogger(__name__)
    settings = load_settings()
    logger.info("[Monitor] Start stream processor...")

    try:
        init_clickhouse()
        topic = "model_monitor"
        spark = create_spark_kafka_minio_clickhouse(app_name="stream_processor", settings=settings)
        spark = spark.getOrCreate()
        spark = prepare_clickhouse_native(spark=spark)
        clickhouse_writer = ClickHouseSink(spark=spark)
        raw_df = read_kafka_stream(spark, settings, topic)

        def process_batch(batch_df, batch_id):
            # Kiểm tra nếu batch không trống
            if not batch_df.isEmpty():
                print(f"[Monitor] --- Processing Batch: {batch_id} ---")

                parsed_df = batch_df.select(col("value").cast("string").alias("raw_data")) \
                    .select(col("raw_data"), from_json(col("raw_data"), MONITOR_SCHEMA).alias("data")) \
                    .select("data.*")

                clickhouse_writer.write_table(parsed_df, settings.storage.clickhouse.table.monitor_table)

        query = raw_df.writeStream \
            .foreachBatch(process_batch) \
            .start()

        query.awaitTermination()

    except Exception as e:
        logger.error("[Monitor] Error when stream processor: %s", e)

if __name__ == "__main__":
    run_stream()