# Model ML

Thư mục chứa mô hình máy học đã được huấn luyện sẵn để sử dụng trong Stream Processor.

---

## Cấu trúc thư mục

```text
model_ml/
├── model/
│   └── gbt_model/       # GBTClassificationModel đã export (PySpark MLlib format)
└── README.md
```

---

## Mô hình: GBT Classifier (Gradient-Boosted Trees)

| Thuộc tính | Giá trị |
| :--- | :--- |
| **Loại model** | `GBTClassifier` (PySpark MLlib) |
| **Bài toán** | Binary Classification (gian lận / hợp lệ) |
| **Feature** | ~100+ features sau khi xử lý từ transaction + identity |
| **Metric đánh giá** | PR-AUC (Area Under Precision-Recall Curve) |
| **Xử lý mất cân bằng** | `classWeight = n_normal / n_fraud` |
| **Hyperparameter chính** | `maxIter=100`, `maxDepth=3`, `stepSize=0.05`, `maxBins=2000` |

---

## Cách model được tạo ra

Model được huấn luyện bởi pipeline tại [`src/batch_layer/train_ml/train_pipeline.py`](../batch_layer/README.md) từ dataset IEEE-CIS Fraud Detection.

Sau khi huấn luyện, model được lưu vào thư mục này để stream processor load trực tiếp:

```python
# Trong stream_processor/main.py
from pyspark.ml.classification import GBTClassificationModel

MODEL_PATH = Path(__file__).parent.parent.parent / "model_ml" / "model" / "gbt_model"
model = GBTClassificationModel.load(str(MODEL_PATH))
```

---

## Tái huấn luyện model

Nếu cần huấn luyện lại và cập nhật model, chạy pipeline batch:

```bash
export PYTHONPATH=$(pwd)/src
python -m batch_layer.train_ml.train_pipeline
```

Sau đó copy model mới về thư mục này:
```bash
# Model được lưu tại s3a://fraud-detection/models/gbt_classifier
# Download về và thay thế src/model_ml/model/gbt_model
```
