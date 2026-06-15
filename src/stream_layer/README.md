# Stream Layer

Tầng xử lý luồng dữ liệu thời gian thực của hệ thống, bao gồm hai thành phần chính: **Ingestion** (đẩy dữ liệu vào Kafka) và **Stream Processor** (tiêu thụ từ Kafka, dự đoán gian lận, ghi vào ClickHouse).

---

## Cấu trúc thư mục

```text
stream_layer/
├── ingestion/           # Kafka Producer — đọc CSV, đẩy lên topic fraud_topic
│   ├── data/            # Dữ liệu đầu vào (test_merged.csv)
│   ├── util/            # Tiện ích bổ trợ
│   ├── data_loader.py   # Generator đọc CSV theo dòng
│   ├── kafka_client.py  # Wrapper KafkaProducer (confluent-kafka)
│   ├── main.py          # Entrypoint ingestion
│   └── Dockerfile
└── stream_processor/    # Spark Structured Streaming — tiêu thụ Kafka, predict, ghi ClickHouse
    ├── processor/
    │   ├── event_processor.py   # Tiền xử lý feature (parse, cast, fill null)
    │   └── event_prediction.py  # Load GBT model, chạy inference
    ├── schema/          # Schema định nghĩa Kafka message
    ├── script/          # Script tiện ích
    ├── feature_schema.csv       # Danh sách feature đầu vào model
    ├── main.py          # Entrypoint stream processor
    └── Dockerfile
```

---

## 1. Ingestion

Đọc file CSV (`test_merged.csv`) và gửi từng record lên Kafka topic `fraud_topic` theo batch.

**Cách chạy (local):**
```bash
export PYTHONPATH=$(pwd)/src
export APP_ENV=dev

python -m stream_layer.ingestion.main
```

**Cơ chế:**
- Đọc từng dòng CSV bằng `csv_data_generator`.
- Gửi từng record qua `KafkaProducer.send(topic, record)`.
- Flush buffer sau mỗi 100 records hoặc khi buffer đầy (`max_buffer`), nghỉ 1 giây giữa các batch để giảm tải.
- Log tiến trình mỗi 100 records.

---

## 2. Stream Processor

Tiêu thụ dữ liệu từ Kafka, thực hiện tiền xử lý và chạy model GBT để dự đoán gian lận, ghi kết quả vào ClickHouse.

**Cách chạy (local):**
```bash
export PYTHONPATH=$(pwd)/src
export APP_ENV=dev

python -m stream_layer.stream_processor.main
```

**Pipeline xử lý (per micro-batch):**
1. `read_kafka_stream` — kết nối Kafka, đọc stream từ topic `fraud_topic`.
2. `event_processor` — parse JSON, cast kiểu dữ liệu, fill null theo `feature_schema.csv`.
3. `event_prediction` — load `GBTClassificationModel` từ `src/model_ml/model/gbt_model`, chạy predict.
4. Gắn thêm `model_id` và `process_timestamp`.
5. `ClickHouseSink.write_table` — ghi vào bảng `predictions.fraud_prediction` trên ClickHouse.

**Chọn model:**
```python
# Trong main.py
MODEL_NUMBER = 1   # 1 → model_1, 2 → model_2
```

---

## Cấu hình liên quan

Xem `src/config/config.dev.yml` (local) hoặc `src/config/config.prod.yml` (K8s):

```yaml
kafka:
  topics:
    topic: fraud_topic
  server:
    bootstrap_servers: localhost:9092   # dev
storage:
  clickhouse:
    table:
      main_table: fraud_prediction
```
