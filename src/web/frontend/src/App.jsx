import { useState, useEffect, useCallback } from 'react'
import { useTheme } from './hooks/useTheme'
import { useWebSocket } from './hooks/useWebSocket'
import { api } from './api/apiClient'

import { ThemeToggle } from './components/ThemeToggle'
import { LiveIndicator } from './components/LiveIndicator'
import { FraudPredictionPanel } from './components/FraudPredictionPanel'
import { ModelMonitorPanel } from './components/ModelMonitorPanel'

// Config model IDs from backend — always show both columns
const FIXED_MODELS = ['model_1', 'model_2']

/* ── Model column header ── */
function ModelColHeader({ modelId, index, hasData, stats }) {
  const colors = ['m1', 'm2']
  const labels = { model_1: 'Model 1', model_2: 'Model 2' }
  const colorClass = colors[index] || 'm1'
  const total = stats?.total ?? 0

  return (
    <div className="col-panel-header">
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
        <span className={`model-badge ${colorClass}`}>{labels[modelId] || modelId}</span>
        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
          {modelId}
        </span>
      </div>
      {hasData && (
        <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
          <strong style={{ color: 'var(--accent-blue)', fontFamily: 'var(--font-mono)' }}>
            {total >= 1000 ? (total / 1000).toFixed(1) + 'K' : total}
          </strong> records
        </span>
      )}
    </div>
  )
}

/* ── Not deployed placeholder ── */
function NotDeployed({ modelId }) {
  return (
    <div className="not-deployed">
      <span className="nd-icon">🚫</span>
      <h3>Model chưa được deploy</h3>
      <p>
        Chưa phát hiện dữ liệu cho <strong style={{ fontFamily: 'var(--font-mono)' }}>{modelId}</strong>
        {' '}trong ClickHouse.
      </p>
      <span className="nd-badge">⏳ Đang chờ dữ liệu…</span>
    </div>
  )
}

/* ── Predictions Tab: 2 fixed columns ── */
function PredictionsTab({ models, snapshot, windowMins }) {
  return (
    <div className="two-col-split">
      {FIXED_MODELS.map((mid, idx) => {
        const hasData = models.includes(mid)
        const stats = snapshot?.models?.[mid]?.prediction_stats
        return (
          <div key={mid} className="col-panel">
            <ModelColHeader modelId={mid} index={idx} hasData={hasData} stats={stats} />
            {hasData ? (
              <div className="col-panel-body">
                <FraudPredictionPanel stats={stats} windowMins={windowMins} />
              </div>
            ) : (
              <NotDeployed modelId={mid} />
            )}
          </div>
        )
      })}
    </div>
  )
}

/* ── Monitor Tab: 2 fixed columns ── */
function MonitorTab({ models, snapshot, windowMins }) {
  return (
    <div className="two-col-split">
      {FIXED_MODELS.map((mid, idx) => {
        const hasData = models.includes(mid)
        const stats = snapshot?.models?.[mid]?.monitor_stats
        return (
          <div key={mid} className="col-panel">
            <ModelColHeader modelId={mid} index={idx} hasData={hasData} stats={stats} />
            {hasData ? (
              <div className="col-panel-body">
                <ModelMonitorPanel stats={stats} windowMins={windowMins} />
              </div>
            ) : (
              <NotDeployed modelId={mid} />
            )}
          </div>
        )
      })}
    </div>
  )
}

/* ── Root App ── */
export default function App() {
  const { theme, toggleTheme } = useTheme()
  const [activeTab, setActiveTab] = useState('predictions')
  const [snapshot, setSnapshot] = useState(null)
  const [scanInterval, setScanInterval] = useState(null)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [windowMins, setWindowMins] = useState(30)
  const [bucketStr, setBucketStr] = useState('1 MINUTE')

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
  }, [theme])

  useEffect(() => {
    api.config().then(c => setScanInterval(c.scan_interval_seconds)).catch(() => { })
  }, [])

  useEffect(() => {
    api.snapshot(windowMins, bucketStr).then(s => setSnapshot(s)).catch(() => { })
  }, [windowMins, bucketStr])

  const handleWsMessage = useCallback((data) => {
    if (windowMins === 30 && bucketStr === '1 MINUTE') {
      setSnapshot(data)
    } else {
      api.snapshot(windowMins, bucketStr).then(s => setSnapshot(s)).catch(() => { })
    }
  }, [windowMins, bucketStr])

  const { connected } = useWebSocket(handleWsMessage)

  const handleRefresh = async () => {
    setIsRefreshing(true)
    try {
      const data = await api.refresh(windowMins, bucketStr)
      setSnapshot(data)
    } catch (e) {
      console.error("Manual refresh failed:", e)
    } finally {
      setIsRefreshing(false)
    }
  }

  const activeModels = snapshot?.active_models ?? []
  const lastUpdated = snapshot?.last_updated ?? null

  return (
    <>
      {/* ── Header ── */}
      <header className="app-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '1.5rem' }}>
          <div className="header-logo">
            <div className="logo-icon">🛡️</div>
            <span className="logo-text">Fraud<span>Shield</span></span>
          </div>

          {/* Main tab switcher */}
          <div className="main-tabs">
            <button
              id="tab-predictions"
              className={`main-tab-btn ${activeTab === 'predictions' ? 'active' : ''}`}
              onClick={() => setActiveTab('predictions')}
            >
              🔍 Dự đoán Realtime
            </button>
            <button
              id="tab-monitor"
              className={`main-tab-btn ${activeTab === 'monitor' ? 'active' : ''}`}
              onClick={() => setActiveTab('monitor')}
            >
              📈 Kết quả Thực tế
            </button>
          </div>
        </div>

        <div className="header-right">
          {scanInterval && (
            <span style={{ fontSize: '0.74rem', color: 'var(--text-muted)' }}>
              Scan: <strong style={{ color: 'var(--accent-blue)', fontFamily: 'var(--font-mono)' }}>{scanInterval}s</strong>
            </span>
          )}

          {/* Time Window Selector */}
          <select
            className="filter-select"
            value={windowMins}
            onChange={(e) => setWindowMins(Number(e.target.value))}
            title="Khoảng thời gian hiển thị"
          >
            <option value={10}>Last 10m</option>
            <option value={30}>Last 30m</option>
            <option value={60}>Last 1h</option>
            <option value={360}>Last 6h</option>
            <option value={1440}>Last 24h</option>
          </select>

          {/* Bucket Size Selector */}
          <select
            className="filter-select"
            value={bucketStr}
            onChange={(e) => setBucketStr(e.target.value)}
            title="Độ chia thời gian"
          >
            <option value="10 SECOND">Gom 10s</option>
            <option value="30 SECOND">Gom 30s</option>
            <option value="1 MINUTE">Gom 1m</option>
            <option value="5 MINUTE">Gom 5m</option>
            <option value="1 HOUR">Gom 1h</option>
          </select>

          <button
            className="refresh-btn"
            onClick={handleRefresh}
            disabled={isRefreshing}
            title="Refresh dữ liệu ngay lập tức"
          >
            {isRefreshing ? '⏳' : '🔄'} Refresh
          </button>
          <LiveIndicator connected={connected} lastUpdated={lastUpdated} />
          <ThemeToggle theme={theme} onToggle={toggleTheme} />
        </div>
      </header>

      {/* ── Tab content ── */}
      <main className="app-main-tabs">
        {activeTab === 'predictions' ? (
          <PredictionsTab models={activeModels} snapshot={snapshot} windowMins={windowMins} />
        ) : (
          <MonitorTab models={activeModels} snapshot={snapshot} windowMins={windowMins} />
        )}
      </main>
    </>
  )
}

