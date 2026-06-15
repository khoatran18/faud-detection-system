# Deployment — Docker

Cấu hình Docker Compose để chạy **hạ tầng cốt lõi** (Kafka + ClickHouse) cho môi trường local/dev.

---

## Services

| Service | Image | Port | Mô tả |
| :--- | :--- | :--- | :--- |
| `broker1` | `apache/kafka:latest` | `9092` (host) / `29092` (internal) | Apache Kafka broker (KRaft mode, không cần Zookeeper) |
| `clickhouse` | `clickhouse/clickhouse-server` | `8123` (HTTP) / `9002→9000` (native) | ClickHouse OLAP database |
| `tabix` | `spoonest/clickhouse-tabix-web-client` | `8081` | Web UI để truy vấn ClickHouse |

---

## Cách chạy

```bash
cd deployment/docker
docker compose -f docker-compose.yml up -d
```

Kiểm tra trạng thái:
```bash
docker compose ps
```

Dừng:
```bash
docker compose down
```

---

## Ghi chú cấu hình Kafka

- **KRaft mode**: Kafka chạy không cần Zookeeper (`KAFKA_PROCESS_ROLES: broker,controller`).
- **3 partitions** mặc định cho mỗi topic mới.
- Log retention: **9000 giây** (~2.5h) và tối đa **2 GB** — phù hợp môi trường test.
- Kết nối từ host: `localhost:9092`
- Kết nối nội bộ container: `broker1:29092`

---

## Ghi chú ClickHouse

- HTTP interface (REST API): `http://localhost:8123`
- Native TCP interface: `localhost:9002` (map từ container port 9000)
- Tabix UI: `http://localhost:8081`

Sau khi khởi động, tạo user và database cho ứng dụng:
```sql
-- Kết nối qua Tabix hoặc clickhouse-client
CREATE DATABASE IF NOT EXISTS predictions;
CREATE USER IF NOT EXISTS fpp_user IDENTIFIED WITH no_password;
GRANT ALL ON predictions.* TO fpp_user;
```

---

## `service-compose.yml`

File compose riêng để chạy các **application services** (ingestion, stream processor, web...) dưới dạng container. Dùng khi muốn test toàn bộ stack bằng Docker mà không cần Kubernetes.
