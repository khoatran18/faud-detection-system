from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json

from stream_layer.stream_processor.schema.event_schema import EVENT_SCHEMA

def event_processor(df: DataFrame):
    """
    Full logic to process event
    """

    df = cast_event(df)
    return df

def cast_event(df: DataFrame):
    """
    Cast event to string and parse it
    """

    return df.select(col("value").cast("string").alias("raw_data")) \
        .select(col("raw_data"), from_json(col("raw_data"), EVENT_SCHEMA).alias("data")) \
        .select("raw_data", "data.*")