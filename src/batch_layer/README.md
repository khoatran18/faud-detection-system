# Batch Layer

Tầng xử lý batch, chịu trách nhiệm huấn luyện mô hình phát hiện gian lận từ dữ liệu lịch sử và lưu model lên MinIO.

---

## Cấu trúc thư mục

```text
batch_layer/
├── train_ml/
│   ├── train_pipeline.py   # Pipeline huấn luyện (PySpark ML)
│   └── train_dag.py        # Airflow DAG kích hoạt train_pipeline
└── Dockerfile
```

---

## Quy trình huấn luyện (`train_pipeline.py`)

Pipeline sử dụng **PySpark ML** đọc dữ liệu từ MinIO (bucket `fraud-detection/raw/`) và thực hiện các bước:

| Bước | Mô tả |
| :--- | :--- |
| **1. Load data** | Đọc `train_identity.csv` + `train_transaction.csv` từ MinIO qua S3A |
| **2. Drop null columns** | Loại bỏ các cột có > 90% giá trị null |
| **3. Inner join** | Ghép hai bảng theo `TransactionID` |
| **4. Fill null** | Điền mean cho số, `UNKNOWN` cho chuỗi |
| **5. String Indexer** | Mã hóa categorical sang số |
| **6. VectorAssembler** | Gộp tất cả feature thành vector `features` |
| **7. Train/Test split** | 70% train, 30% test (`seed=42`) |
| **8. Xử lý mất cân bằng** | Tính `classWeight = n_normal / n_fraud` |
| **9. Huấn luyện** | **Linear SVC** (có StandardScaler) + **GBT Classifier** |
| **10. Đánh giá** | Metric: **PR-AUC** (phù hợp với dữ liệu mất cân bằng) |
| **11. Lưu model** | Ghi model lên `s3a://fraud-detection/models/` |

**Model được dùng trong production:** `GBTClassificationModel` (lưu tại `src/model_ml/model/gbt_model`).

---

## Cách chạy

### Chạy local (standalone)

```bash
export PYTHONPATH=$(pwd)/src
export APP_ENV=dev

python -m batch_layer.train_ml.train_pipeline
```

> **Lưu ý:** Local cần MinIO đang chạy (xem `deployment/docker/`), bucket `fraud-detection` đã được tạo và upload dữ liệu từ `src/train_data/`.

### Chạy qua Airflow DAG

```bash
# DAG ID: fraud_detection_train
# Trigger thủ công trên Airflow UI hoặc CLI:
airflow dags trigger fraud_detection_train
```

---

## Dữ liệu huấn luyện

Dữ liệu huấn luyện nằm tại `src/train_data/` (IEEE-CIS Fraud Detection dataset):

| File | Kích thước | Mô tả |
| :--- | :--- | :--- |
| `train_transaction.csv` | ~653 MB | Thông tin giao dịch (590 features) |
| `train_identity.csv` | ~26 MB | Thông tin định danh thiết bị/người dùng |

Cần upload lên MinIO trước khi chạy training:
```bash
# Qua MinIO Console (http://localhost:9002 sau khi chạy docker-compose)
# Bucket: fraud-detection, prefix: raw/
```
