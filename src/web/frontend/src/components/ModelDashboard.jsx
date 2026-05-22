import { FraudPredictionPanel } from './FraudPredictionPanel'
import { ModelMonitorPanel } from './ModelMonitorPanel'

const MODEL_COLORS = ['m1', 'm2']
const MODEL_LABELS = { model_1: 'Model 1', model_2: 'Model 2' }

export function ModelDashboard({ modelId, modelIndex, predictionStats, monitorStats }) {
  const colorClass = MODEL_COLORS[modelIndex % 2]
  const label = MODEL_LABELS[modelId] || modelId

  return (
    <div className="model-dashboard" id={`dashboard-${modelId}`}>
      <div className="model-header">
        <div className="model-title">
          <span className={`model-badge ${colorClass}`}>{label}</span>
          <span style={{ fontSize: '0.82rem', color: 'var(--text-secondary)', fontFamily: 'var(--font-mono)' }}>
            ID: {modelId}
          </span>
        </div>
        <div style={{ display: 'flex', gap: '1.5rem', fontSize: '0.78rem', color: 'var(--text-muted)' }}>
          <span>🔍 {predictionStats?.total?.toLocaleString() ?? 0} predictions</span>
          <span>📈 {monitorStats?.total?.toLocaleString() ?? 0} monitored</span>
        </div>
      </div>

      <FraudPredictionPanel stats={predictionStats} />
      <ModelMonitorPanel stats={monitorStats} />
    </div>
  )
}
