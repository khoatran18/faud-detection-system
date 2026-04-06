from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType

EVENT_SCHEMA = StructType([
    StructField("TransactionID", LongType(), True),
])
