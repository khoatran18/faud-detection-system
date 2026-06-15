# Model Monitor

Tầng giám sát model, chịu trách nhiệm tạo dữ liệu monitoring (nhãn thực tế vs dự đoán), đẩy lên Kafka topic riêng (`model_monitor`), và xử lý kết quả vào ClickHouse để dashboard theo dõi hiệu năng model.

---

## Cấu trúc thư mục

```text
model_monitor/
├── generate_data.py            # Sinh dữ liệu monitor giả lập (label thực vs predict)
├── monitor_ingestion.py        # Kafka Producer — đẩy dữ liệu monitor lên topic model_monitor
├── monitor_stream_processor.py # Spark Streaming — tiêu thụ model_monitor, ghi vào ClickHouse
└── Dockerfile
```

---

## Schema dữ liệu monitor

Mỗi record gửi lên topic `model_monitor` có cấu trúc:

| Field | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `TransactionID` | Long | ID giao dịch |
| `model_id` | String | ID của model đang giám sát |
| `model_predict` | Int | Nhãn model dự đoán (0 / 1) |
| `actual_result` | Int | Nhãn thực tế (ground truth) |
| `is_correct` | Int | 1 nếu dự đoán đúng, 0 nếu sai |
| `process_timestamp` | Timestamp | Thời điểm xử lý |

Kết quả ghi vào bảng `predictions.model_monitor` trên ClickHouse.

---

## Cách chạy

### Monitor Ingestion (Producer)

```bash
export PYTHONPATH=$(pwd)/src
export APP_ENV=dev

python -m model_monitor.monitor_ingestion
```

### Monitor Stream Processor (Consumer)

```bash
export PYTHONPATH=$(pwd)/src
export APP_ENV=dev

python -m model_monitor.monitor_stream_processor
```

---

## Mối quan hệ với Web Dashboard

Dữ liệu từ bảng `predictions.model_monitor` được Web Dashboard poll theo chu kỳ và hiển thị:
- **Accuracy**, **Precision**, **Recall**, **F1 Score** tổng hợp.
- **Confusion matrix** (TP / FP / TN / FN).
- **Accuracy timeline** (biểu đồ 24h).

Xem thêm tại: [`src/web/README.md`](../web/README.md).
