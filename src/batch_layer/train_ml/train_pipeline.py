# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col as F_col
from pyspark.sql.types import (IntegerType, DoubleType, FloatType,
                               LongType, ShortType, ByteType,
                               DecimalType, StringType)
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import LinearSVC, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import time


def train_pipeline():
    print("=== Khởi tạo Spark Session & Kết nối MinIO ===", flush=True)

    # Cấu hình cố định Endpoint trỏ tới service nội bộ K8s chéo namespace (fpp -> data-platform)
    MINIO_ENDPOINT = "http://minio-internal.fpp.svc.cluster.local:9000"
    MINIO_ACCESS_KEY = "minio"
    MINIO_SECRET_KEY = "minio123"

    spark = SparkSession.builder \
        .appName("FraudDetection_MinIO_Demo") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Đường dẫn S3A đọc từ Bucket 'fraud-detection'
    identity_path = "s3a://fraud-detection/raw/train_identity.csv"
    transaction_path = "s3a://fraud-detection/raw/train_transaction.csv"

    print("--> Đang tải dữ liệu từ MinIO...", flush=True)
    identity_df = spark.read.csv(identity_path, header=True, inferSchema=True).limit(500)
    transaction_df = spark.read.csv(transaction_path, header=True, inferSchema=True).limit(500)

    print("=" * 60, flush=True)
    print(f"Identity    : {identity_df.count():>8,} rows | {len(identity_df.columns)} cols", flush=True)
    print(f"Transaction : {transaction_df.count():>8,} rows | {len(transaction_df.columns)} cols", flush=True)

    # 2. Drop null columns (>90% null)
    def drop_null_columns(df, threshold=0.9, label=""):
        total_rows = df.count()
        null_counts = df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in df.columns
        ]).collect()[0].asDict()

        cols_to_drop = [c for c, cnt in null_counts.items() if (cnt / total_rows) > threshold]
        df = df.drop(*cols_to_drop)
        print(f"[{label}] Dropped {len(cols_to_drop)} cols → còn {len(df.columns)} cols", flush=True)
        return df

    print("\n--- Drop null columns ---", flush=True)
    transaction_df = drop_null_columns(transaction_df, label="TRANSACTION")
    identity_df = drop_null_columns(identity_df, label="IDENTITY")

    # 3. Inner join
    joined_df = transaction_df.join(identity_df, on="TransactionID", how="inner")
    print(f"\nJoined : {joined_df.count():,} rows | {len(joined_df.columns)} cols", flush=True)

    # 4. Fill null
    SKIP = {"TransactionID", "isFraud"}
    numerical_types = (IntegerType, DoubleType, FloatType, LongType, ShortType, ByteType, DecimalType)

    num_cols = [f.name for f in joined_df.schema.fields if
                isinstance(f.dataType, numerical_types) and f.name not in SKIP]
    str_cols = [f.name for f in joined_df.schema.fields if isinstance(f.dataType, StringType) and f.name not in SKIP]

    mean_row = joined_df.agg(*[F.mean(c).alias(c) for c in num_cols]).collect()[0].asDict()
    fill_mean = {c: v for c, v in mean_row.items() if v is not None}
    fill_minus1 = {c: -1.0 for c in mean_row if mean_row[c] is None}

    joined_df = (joined_df
                 .fillna(fill_mean)
                 .fillna(fill_minus1)
                 .fillna({c: "UNKNOWN" for c in str_cols}))

    # 5. String Indexer (Mã hóa categorical)
    encoded_df = joined_df
    for c in str_cols:
        indexer = StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep")
        encoded_df = indexer.fit(encoded_df).transform(encoded_df)

    encoded_df = encoded_df.drop(*str_cols)
    for c in str_cols:
        encoded_df = encoded_df.withColumnRenamed(c + "_idx", c)

    # 6. Assembler đặc trưng
    feature_cols = [c for c in encoded_df.columns if c not in SKIP]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

    ml_df = (assembler
             .transform(encoded_df)
             .select("features", F_col("isFraud").cast("double").alias("label")))

    # Train/Test split 70/30
    train_df, test_df = ml_df.randomSplit([0.7, 0.3], seed=42)

    # Xử lý mất cân bằng class
    total = train_df.count()
    n_fraud = train_df.filter(F_col("label") == 1).count()
    n_normal = total - n_fraud
    weight = n_normal / n_fraud

    train_weighted = train_df.withColumn("classWeight", F.when(F_col("label") == 1, float(weight)).otherwise(1.0))

    # --- Huấn luyện Linear SVC ---
    print("\n--- Huấn luyện & Đánh giá Linear SVC (PR-AUC) ---", flush=True)
    t0 = time.time()
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True)
    scaler_model = scaler.fit(train_weighted)
    train_scaled = scaler_model.transform(train_weighted)
    test_scaled = scaler_model.transform(test_df)

    lsvc = LinearSVC(featuresCol="scaledFeatures", labelCol="label", weightCol="classWeight", maxIter=150, regParam=0.1,
                     standardization=False)
    svc_model = lsvc.fit(train_scaled)
    print(f"Thời gian huấn luyện SVC: {time.time() - t0:.1f} giây.", flush=True)

    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
    svc_pr_auc = evaluator.evaluate(svc_model.transform(test_scaled))
    print(f"==> Linear SVC Test PR-AUC: {svc_pr_auc:.4f}", flush=True)

    # --- Huấn luyện GBT Classifier ---
    print("\n--- Huấn luyện & Đánh giá GBT Classifier (PR-AUC) ---", flush=True)
    t0 = time.time()
    gbt = GBTClassifier(featuresCol="features", labelCol="label", weightCol="classWeight", maxIter=100, maxDepth=3,
                        stepSize=0.05, maxBins=2000, seed=42)
    gbt_model = gbt.fit(train_weighted)
    print(f"Thời gian huấn luyện GBT: {time.time() - t0:.1f} giây.", flush=True)

    gbt_pr_auc = evaluator.evaluate(gbt_model.transform(test_df))
    print(f"==> GBT Classifier Test PR-AUC: {gbt_pr_auc:.4f}", flush=True)

    print("\n--> Đang lưu trữ mô hình vào MinIO...", flush=True)
    svc_model.write().overwrite().save("s3a://fraud-detection/models/linear_svc")
    gbt_model.write().overwrite().save("s3a://fraud-detection/models/gbt_classifier")
    print("==> Lưu mô hình thành công!", flush=True)

    spark.stop()
    return svc_model, gbt_model


if __name__ == "__main__":
    train_pipeline()