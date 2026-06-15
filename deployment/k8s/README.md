# Deployment — Kubernetes

Manifest Kubernetes để triển khai toàn bộ hệ thống lên **Minikube** (hoặc bất kỳ K8s cluster nào) trong namespace `fpp`.

---

## Cấu trúc thư mục

```text
k8s/
├── base/
│   ├── 01-namespace.yaml        # Namespace: fpp
│   └── 02-storageclass.yaml     # StorageClass cho PersistentVolume
├── storage/
│   ├── 01-minio.yaml            # MinIO StatefulSet + Services
│   ├── 02-clickhouse.yaml       # ClickHouse StatefulSet + Services (với ConfigMap fpp_user)
│   └── 03-minio-bootstrap.yaml  # Job khởi tạo bucket trên MinIO
├── services/
│   ├── 01-kafka.yaml            # Kafka Deployment + Service (KRaft mode)
│   ├── 02-stream-ingestion.yaml # stream-ingestion Deployment
│   ├── 03-stream-processor.yaml # stream-processor Deployment
│   ├── 04-monitor-ingestion.yaml# monitor-ingestion Deployment
│   ├── 05-monitor-processor.yaml# monitor-processor Deployment
│   └── 06-web.yaml              # web-ui Deployment + Service
├── secrets/                     # (Không commit) Kubernetes Secrets
└── script.txt                   # Lệnh hay dùng với minikube/kubectl
```

---

## Thứ tự triển khai

```bash
# 1. Khởi động Minikube
minikube start --driver=docker --cpus=10 --memory=16384

# 2. (Nếu cần mount dữ liệu training)
minikube mount /path/to/project/src/train_data:/data/train_data

# 3. Base (namespace + storage class)
kubectl apply -f deployment/k8s/base/

# 4. Storage (MinIO, ClickHouse)
kubectl apply -f deployment/k8s/storage/

# 5. Services (Kafka + Application services)
kubectl apply -f deployment/k8s/services/
```

---

## Services và DNS nội bộ

| Service | DNS nội bộ (cluster) | Port |
| :--- | :--- | :--- |
| Kafka | `kafka-internal.fpp.svc.cluster.local` | `29092` |
| ClickHouse HTTP | `clickhouse-internal.fpp.svc.cluster.local` | `8123` |
| ClickHouse Native | `clickhouse-internal.fpp.svc.cluster.local` | `9000` |
| MinIO API | `minio-internal.fpp.svc.cluster.local` | `9000` |
| Web UI | `web-ui.fpp.svc.cluster.local` | `8000` |

---

## Truy cập từ local

```bash
# Truy cập Web Dashboard
kubectl port-forward svc/web-ui 8000:8000 -n fpp &

# Truy cập ClickHouse (cho debug)
kubectl port-forward svc/clickhouse-internal 8123:8123 -n fpp &

# Truy cập Tabix UI
kubectl port-forward svc/tabix-external 8081:80 -n fpp &
```

Sau đó mở `http://localhost:8000` để xem dashboard.

---

## Kiểm tra trạng thái

```bash
# Xem tất cả pods trong namespace fpp
kubectl get pods -n fpp

# Xem logs của một pod
kubectl logs -f <pod-name> -n fpp

# Restart một deployment
kubectl rollout restart deployment/<name> -n fpp
```
