import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'

function pct(v) { return ((v || 0) * 100).toFixed(1) + '%' }

function MetricBar({ name, value, color }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.2rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem' }}>
        <span style={{ color: 'var(--text-secondary)' }}>{name}</span>
        <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, color }}>{pct(value)}</span>
      </div>
      <div style={{ height: 5, borderRadius: 999, background: 'var(--border)', overflow: 'hidden' }}>
        <div style={{ height: '100%', width: `${(value || 0) * 100}%`, background: color, borderRadius: 999, transition: 'width 0.6s ease' }} />
      </div>
    </div>
  )
}

function TooltipContent({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div style={{ background: 'var(--bg-modal)', border: '1px solid var(--border-hover)', borderRadius: 8, padding: '0.5rem 0.75rem', fontSize: '0.75rem' }}>
      <p style={{ color: 'var(--text-secondary)', marginBottom: 3 }}>{label}</p>
      {payload.map(p => (
        <p key={p.name} style={{ color: p.stroke || p.color }}>
          {p.name}: <strong>{p.value?.toFixed(1)}%</strong>
        </p>
      ))}
    </div>
  )
}

export function ModelMonitorPanel({ stats, windowMins = 30 }) {
  const {
    total = 0, accuracy = 0, precision = 0, recall = 0, f1 = 0,
    confusion_matrix: cm = { tp: 0, fp: 0, tn: 0, fn: 0 },
    timeline = [],
  } = stats || {}

  const windowLabel = windowMins >= 60 ? `${(windowMins / 60).toFixed(0)}h` : `${windowMins}m`

  const tlData = timeline.map(t => {
    const utcStr = t.hour_bucket.includes('T') ? t.hour_bucket : t.hour_bucket.replace(' ', 'T') + 'Z'
    const timeLabel = new Date(utcStr).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    const t_total = t.total || 0
    const t_correct = t.correct || 0
    const t_tp = t.tp || 0
    const t_fp = t.fp || 0
    const t_fn = t.fn || 0

    const acc = t_total > 0 ? (t_correct / t_total) * 100 : 0.0
    const prec = (t_tp + t_fp) > 0 ? (t_tp / (t_tp + t_fp)) * 100 : 0.0
    const rec = (t_tp + t_fn) > 0 ? (t_tp / (t_tp + t_fn)) * 100 : 0.0
    const f1s = (prec + rec) > 0 ? (2 * prec * rec) / (prec + rec) : 0.0

    return {
      time: timeLabel,
      Accuracy: acc,
      Precision: prec,
      Recall: rec,
      'F1 Score': f1s,
    }
  })

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.85rem', flex: 1, height: '100%', minHeight: 0 }}>
      {/* KPI row */}
      <div className="kpi-row" style={{ flexShrink: 0 }}>
        <div className="kpi-mini blue">
          <span className="km-label">Total Monitored</span>
          <span className="km-value" style={{ color: 'var(--accent-blue)' }}>
            {total >= 1000 ? (total / 1000).toFixed(1) + 'K' : total}
          </span>
        </div>
        <div className="kpi-mini legit">
          <span className="km-label">Accuracy</span>
          <span className="km-value" style={{ color: 'var(--accent-green)' }}>{pct(accuracy)}</span>
        </div>
        <div className="kpi-mini purple">
          <span className="km-label">F1 Score</span>
          <span className="km-value" style={{ color: 'var(--accent-purple)' }}>{pct(f1)}</span>
        </div>
      </div>

      {/* Confusion matrix + metrics */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem', flexShrink: 0 }}>
        <div className="chart-box">
          <p className="chart-box-title">Confusion Matrix</p>
          <div className="cm-grid">
            <div className="cm-box tp"><span className="cb-val">{cm.tp}</span><span className="cb-lbl">TP</span></div>
            <div className="cm-box fp"><span className="cb-val">{cm.fp}</span><span className="cb-lbl">FP</span></div>
            <div className="cm-box fn"><span className="cb-val">{cm.fn}</span><span className="cb-lbl">FN</span></div>
            <div className="cm-box tn"><span className="cb-val">{cm.tn}</span><span className="cb-lbl">TN</span></div>
          </div>
        </div>
        <div className="chart-box" style={{ display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
          <p className="chart-box-title">Metrics</p>
          <MetricBar name="Accuracy"  value={accuracy}  color="var(--accent-blue)"   />
          <MetricBar name="Precision" value={precision} color="var(--accent-purple)"  />
          <MetricBar name="Recall"    value={recall}    color="var(--accent-orange)"  />
          <MetricBar name="F1"        value={f1}        color="var(--accent-green)"   />
        </div>
      </div>

      {/* 2x2 Grid of 4 Metric Timelines */}
      {tlData.length > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gridTemplateRows: '1fr 1fr', gap: '0.75rem', flex: 1, minHeight: 0 }}>
          {/* Accuracy Timeline */}
          <div className="chart-box" style={{ display: 'flex', flexDirection: 'column', minHeight: 0 }}>
            <p className="chart-box-title" style={{ color: 'var(--accent-blue)' }}>Accuracy (%) — Last {windowLabel}</p>
            <div style={{ flex: 1, minHeight: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={tlData} margin={{ top: 2, right: 2, left: -24, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="time" tick={{ fontSize: 8, fill: 'var(--text-muted)' }} />
                  <YAxis domain={[0, 100]} tick={{ fontSize: 8, fill: 'var(--text-muted)' }} />
                  <Tooltip content={<TooltipContent />} />
                  <Line type="monotone" dataKey="Accuracy" stroke="var(--accent-blue)" strokeWidth={1.5} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Precision Timeline */}
          <div className="chart-box" style={{ display: 'flex', flexDirection: 'column', minHeight: 0 }}>
            <p className="chart-box-title" style={{ color: 'var(--accent-purple)' }}>Precision (%) — Last {windowLabel}</p>
            <div style={{ flex: 1, minHeight: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={tlData} margin={{ top: 2, right: 2, left: -24, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="time" tick={{ fontSize: 8, fill: 'var(--text-muted)' }} />
                  <YAxis domain={[0, 100]} tick={{ fontSize: 8, fill: 'var(--text-muted)' }} />
                  <Tooltip content={<TooltipContent />} />
                  <Line type="monotone" dataKey="Precision" stroke="var(--accent-purple)" strokeWidth={1.5} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Recall Timeline */}
          <div className="chart-box" style={{ display: 'flex', flexDirection: 'column', minHeight: 0 }}>
            <p className="chart-box-title" style={{ color: 'var(--accent-orange)' }}>Recall (%) — Last {windowLabel}</p>
            <div style={{ flex: 1, minHeight: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={tlData} margin={{ top: 2, right: 2, left: -24, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="time" tick={{ fontSize: 8, fill: 'var(--text-muted)' }} />
                  <YAxis domain={[0, 100]} tick={{ fontSize: 8, fill: 'var(--text-muted)' }} />
                  <Tooltip content={<TooltipContent />} />
                  <Line type="monotone" dataKey="Recall" stroke="var(--accent-orange)" strokeWidth={1.5} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* F1 Score Timeline */}
          <div className="chart-box" style={{ display: 'flex', flexDirection: 'column', minHeight: 0 }}>
            <p className="chart-box-title" style={{ color: 'var(--accent-green)' }}>F1 Score (%) — Last {windowLabel}</p>
            <div style={{ flex: 1, minHeight: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={tlData} margin={{ top: 2, right: 2, left: -24, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="time" tick={{ fontSize: 8, fill: 'var(--text-muted)' }} />
                  <YAxis domain={[0, 100]} tick={{ fontSize: 8, fill: 'var(--text-muted)' }} />
                  <Tooltip content={<TooltipContent />} />
                  <Line type="monotone" dataKey="F1 Score" stroke="var(--accent-green)" strokeWidth={1.5} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}


