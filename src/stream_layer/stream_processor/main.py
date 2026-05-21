import logging
from pathlib import Path

from pyspark.ml.classification import GBTClassificationModel
from pyspark.sql.functions import lit, current_timestamp

from common.logging.logging_config import setup_logging
from common.runtime.clickhouse.clickhouse_init import init_clickhouse
from common.runtime.clickhouse.prepare_clickhouse_native import prepare_clickhouse_native
from common.runtime.spark.spark_builder_minio_clickhouse import create_spark_kafka_minio_clickhouse
from common.sinks.clickhouse_sink import ClickHouseSink
from common.sources.kafka_source import read_kafka_stream
from config.settings import load_settings
from stream_layer.stream_processor.processor.event_prediction import event_prediction
from stream_layer.stream_processor.processor.event_processor import event_processor

MODEL_PATH = Path(__file__).parent.parent.parent / "model_ml" / "model" / "gbt_model"
MODEL_NUMBER = 1

def run_stream():
    # Init logging and Kafka producer
    setup_logging()
    logger = logging.getLogger(__name__)
    settings = load_settings()
    logger.info("Start stream processor...")

    if MODEL_NUMBER == 1:
        model_id = settings.model.model_1.id
    else:
        model_id = settings.model.model_2.id

    try:
        init_clickhouse()
        topic = settings.kafka.topics.topic
        spark = create_spark_kafka_minio_clickhouse(app_name="stream_processor", settings=settings)
        spark = spark.getOrCreate()
        spark = prepare_clickhouse_native(spark=spark)
        clickhouse_writer = ClickHouseSink(spark=spark)
        raw_df = read_kafka_stream(spark, settings, topic)

        # Preprocess data
        # preprocess_df = event_processor(raw_df)

        # Predict
        model = GBTClassificationModel.load(str(MODEL_PATH))
        # prediction_df = event_prediction(preprocess_df, model)

        def process_batch(batch_df, batch_id):
            # Kiểm tra nếu batch không trống
            if not batch_df.isEmpty():
                print(f"--- Processing Batch: {batch_id} ---")

                preprocess_df = event_processor(batch_df)
                prediction_df = event_prediction(preprocess_df, model)

                prediction_df.select("TransactionID", "prediction", "probability").show(truncate=False)

                prediction_df.withColumn("model_id", lit(model_id)) \
                    .withColumn("process_timestamp", current_timestamp())

                clickhouse_writer.write_table(prediction_df, settings.storage.clickhouse.table.main_table)

        query = raw_df.writeStream \
            .foreachBatch(process_batch) \
            .start()

        query.awaitTermination()

    except Exception as e:
        logger.error("Error when stream processor: %s", e)

if __name__ == "__main__":
    run_stream()