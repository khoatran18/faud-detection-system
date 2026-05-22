# FraudShield Web Dashboard

Real-time fraud detection visualization dashboard for the BigData pipeline.

## Architecture

```
src/web/
├── backend/       # Python FastAPI — polls ClickHouse, WebSocket broadcast
└── frontend/      # React + Vite — real-time dashboard with dark/light mode
```

## Configuration

Scan interval and ClickHouse settings are loaded from `src/config/config.dev.yml`:

```yaml
web:
  scan_interval_seconds: 10   # ← change this to adjust polling frequency
  host: "0.0.0.0"
  port: 8000
```

Set `APP_ENV=prod` to load `config.prod.yml` instead.

---

## Running the Backend

```bash
cd src/web/backend

# Install dependencies (first time)
pip install -r requirements.txt

# Start the API server
APP_ENV=dev uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

API available at: http://localhost:8000
- `GET /api/health` — health check
- `GET /api/config` — scan interval, known model IDs
- `GET /api/snapshot` — latest in-memory data snapshot
- `GET /api/models` — active model IDs detected in ClickHouse
- `GET /api/stats/{model_id}` — aggregated prediction + monitor stats
- `GET /api/predictions/{model_id}?limit=50&offset=0` — paginated fraud_prediction rows
- `GET /api/monitor/{model_id}?limit=50&offset=0` — paginated model_monitor rows
- `WS /ws` — WebSocket stream (pushes snapshot every `scan_interval_seconds`)

---

## Running the Frontend

```bash
cd src/web/frontend

# Install dependencies (first time)
npm install

# Start dev server
npm run dev
```

Dashboard available at: http://localhost:5173

### Build for production

```bash
npm run build
# Static files output to dist/
```

---

## Dashboard Features

- 🌙 **Dark / ☀️ Light mode** toggle (persisted in localStorage)
- **Auto-detects 1 or 2 models** — layout adjusts automatically
  - 1 model → full-width dashboard
  - 2 models → side-by-side dashboards
- **Per-model dashboards:**
  - 📊 Fraud Predictions (from `predictions.fraud_prediction`)
    - KPI cards: Total scored, Fraud detected, Legitimate
    - Area chart: Timeline (last 24h)
    - Radial gauge: Fraud rate %
  - 📈 Model Monitor (from `predictions.model_monitor`)
    - KPI cards: Total monitored, Accuracy, F1 Score
    - Confusion matrix (TP/FP/TN/FN)
    - Metric bars: Accuracy / Precision / Recall / F1
    - Line chart: Accuracy over time (last 24h)
- **Recent Transactions table** with pagination & model filter
  - Tab: Predictions | Monitor
  - Filter by model_id
- 🔴 Live indicator with WebSocket auto-reconnect
