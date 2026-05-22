import { useState, useEffect, useCallback } from 'react'
import { api } from '../api/apiClient'

const PAGE_SIZE = 20

function formatDate(str) {
  if (!str) return '—'
  try { return new Date(str).toLocaleString() } catch { return str }
}

function formatProb(prob) {
  if (!prob || !Array.isArray(prob)) return '—'
  return prob.map(p => (p * 100).toFixed(1) + '%').join(' / ')
}

export function RecentTransactionsTable({ activeModels }) {
  const [tab, setTab] = useState('predictions') // 'predictions' | 'monitor'
  const [modelFilter, setModelFilter] = useState('all')
  const [page, setPage] = useState(0)
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(false)

  const effectiveModels = activeModels?.length > 0 ? activeModels : []
  const displayModels = modelFilter === 'all' ? effectiveModels : [modelFilter]

  const fetchData = useCallback(async () => {
    if (displayModels.length === 0) return
    setLoading(true)
    try {
      const offset = page * PAGE_SIZE
      if (tab === 'predictions') {
        // fetch for all filtered models and merge
        const results = await Promise.all(
          displayModels.map(m => api.predictions(m, PAGE_SIZE, offset))
        )
        const merged = results.flatMap(r => r.data || [])
        merged.sort((a, b) => new Date(b.process_timestamp) - new Date(a.process_timestamp))
        setRows(merged.slice(0, PAGE_SIZE))
      } else {
        const results = await Promise.all(
          displayModels.map(m => api.monitor(m, PAGE_SIZE, offset))
        )
        const merged = results.flatMap(r => r.data || [])
        merged.sort((a, b) => new Date(b.process_timestamp) - new Date(a.process_timestamp))
        setRows(merged.slice(0, PAGE_SIZE))
      }
    } catch (e) {
      console.error('fetch error', e)
    } finally {
      setLoading(false)
    }
  }, [tab, modelFilter, page, effectiveModels.join(',')])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  // Reset page on filter change
  useEffect(() => { setPage(0) }, [tab, modelFilter])

  return (
    <div className="recent-section">
      <div className="recent-header">
        <div className="flex items-center gap-md">
          <div className="section-icon blue">📋</div>
          <span className="section-title">Recent Transactions</span>
        </div>

        <div className="flex items-center gap-md">
          {/* Model filter */}
          {effectiveModels.length > 1 && (
            <select
              id="model-filter-select"
              className="filter-select"
              value={modelFilter}
              onChange={e => setModelFilter(e.target.value)}
            >
              <option value="all">All Models</option>
              {effectiveModels.map(m => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
          )}

          {/* Tab switcher */}
          <div className="tab-switch">
            <button
              id="tab-predictions"
              className={`tab-btn ${tab === 'predictions' ? 'active' : ''}`}
              onClick={() => setTab('predictions')}
            >
              Predictions
            </button>
            <button
              id="tab-monitor"
              className={`tab-btn ${tab === 'monitor' ? 'active' : ''}`}
              onClick={() => setTab('monitor')}
            >
              Monitor
            </button>
          </div>

          {/* Refresh */}
          <button
            id="refresh-table-btn"
            className="page-btn"
            onClick={fetchData}
            disabled={loading}
            title="Refresh"
          >
            {loading ? '⏳' : '🔄'}
          </button>
        </div>
      </div>

      <div className="recent-body">
        {loading ? (
          <div className="empty-state">
            <div className="loading-spinner" />
          </div>
        ) : rows.length === 0 ? (
          <div className="empty-state">
            <span className="icon">🔎</span>
            <p>No data available</p>
            <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}>
              Waiting for ClickHouse data…
            </p>
          </div>
        ) : tab === 'predictions' ? (
          <PredictionsTable rows={rows} />
        ) : (
          <MonitorTable rows={rows} />
        )}
      </div>

      <div className="recent-footer">
        <div className="pagination">
          <button
            id="prev-page-btn"
            className="page-btn"
            onClick={() => setPage(p => Math.max(0, p - 1))}
            disabled={page === 0}
          >
            ← Prev
          </button>
          <span className="page-btn active">Page {page + 1}</span>
          <button
            id="next-page-btn"
            className="page-btn"
            onClick={() => setPage(p => p + 1)}
            disabled={rows.length < PAGE_SIZE}
          >
            Next →
          </button>
        </div>
      </div>
    </div>
  )
}

function PredictionsTable({ rows }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>Transaction ID</th>
          <th>Model ID</th>
          <th>Prediction</th>
          <th>Probability</th>
          <th>Amount</th>
          <th>Timestamp</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={`${r.TransactionID}-${i}`} className={r.prediction === 1 ? 'fraud-row' : ''}>
            <td>{r.TransactionID}</td>
            <td>
              <span className="badge badge-model">{r.model_id}</span>
            </td>
            <td>
              {r.prediction === 1
                ? <span className="badge badge-fraud">🚨 Fraud</span>
                : <span className="badge badge-legit">✅ Legit</span>
              }
            </td>
            <td>{formatProb(r.probability)}</td>
            <td>{r.TransactionAmt != null ? `$${Number(r.TransactionAmt).toFixed(2)}` : '—'}</td>
            <td>{formatDate(r.process_timestamp)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function MonitorTable({ rows }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>Transaction ID</th>
          <th>Model ID</th>
          <th>Model Predict</th>
          <th>Actual Result</th>
          <th>Correct?</th>
          <th>Timestamp</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={`${r.TransactionID}-${i}`} className={!r.is_correct ? 'fraud-row' : ''}>
            <td>{r.TransactionID}</td>
            <td>
              <span className="badge badge-model">{r.model_id}</span>
            </td>
            <td>
              {r.model_predict === 1
                ? <span className="badge badge-fraud">Fraud</span>
                : <span className="badge badge-legit">Legit</span>
              }
            </td>
            <td>
              {r.actual_result === 1
                ? <span className="badge badge-fraud">Fraud</span>
                : <span className="badge badge-legit">Legit</span>
              }
            </td>
            <td>
              {r.is_correct
                ? <span className="badge badge-correct">✓ Correct</span>
                : <span className="badge badge-wrong">✗ Wrong</span>
              }
            </td>
            <td>{formatDate(r.process_timestamp)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
