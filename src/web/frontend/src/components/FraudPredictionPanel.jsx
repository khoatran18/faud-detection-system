import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'

function fmt(n) {
  if (n == null || n === 0) return '0'
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return n.toLocaleString()
}

function TooltipContent({ active, payload, label, percent }) {
  if (!active || !payload?.length) return null
  return (
    <div style={{ background: 'var(--bg-modal)', border: '1px solid var(--border-hover)', borderRadius: 8, padding: '0.5rem 0.75rem', fontSize: '0.75rem' }}>
      <p style={{ color: 'var(--text-secondary)', marginBottom: 3 }}>{label}</p>
      {payload.map(p => (
        <p key={p.name} style={{ color: p.color || p.stroke }}>
          {p.name}: <strong>{percent ? p.value?.toFixed(2) + '%' : p.value?.toLocaleString()}</strong>
        </p>
      ))}
    </div>
  )
}

function GaugeSVG({ value }) {
  const pct = Math.min(Math.max((value || 0) * 100, 0), 100)
  const r = 38; const circ = 2 * Math.PI * r
  const dash = (pct / 100) * circ * 0.75
  const off = -circ * 0.125
  const color = 'var(--fraud-color)'
  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: '0.4rem', width: '100%', height: '100%' }}>
      <svg width="140" height="95" viewBox="0 0 100 70">
        <circle cx="50" cy="58" r={r} fill="none" stroke="var(--border)" strokeWidth="8"
          strokeDasharray={`${circ * 0.75} ${circ}`} strokeDashoffset={off}
          strokeLinecap="round" transform="rotate(-225 50 58)" />
        <circle cx="50" cy="58" r={r} fill="none" stroke={color} strokeWidth="8"
          strokeDasharray={`${dash} ${circ}`} strokeDashoffset={off}
          strokeLinecap="round" transform="rotate(-225 50 58)"
          style={{ transition: 'stroke-dasharray 0.6s ease', filter: `drop-shadow(0 0 5px ${color})` }} />
        <text x="50" y="52" textAnchor="middle" fill="var(--text-primary)"
          fontSize="15" fontWeight="800" fontFamily="JetBrains Mono,monospace">
          {pct.toFixed(1)}%
        </text>
      </svg>
      <span style={{ fontSize: '0.74rem', fontWeight: 800, textTransform: 'uppercase', letterSpacing: '0.08em', color: color }}>Fraud Rate</span>
    </div>
  )
}

export function FraudPredictionPanel({ stats, windowMins = 30 }) {
  const { total = 0, fraud_count = 0, legit_count = 0, fraud_rate = 0, timeline = [] } = stats || {}

  const windowLabel = windowMins >= 60 ? `${(windowMins / 60).toFixed(0)}h` : `${windowMins}m`

  const chartData = timeline.map(t => {
    const utcStr = t.hour_bucket.includes('T') ? t.hour_bucket : t.hour_bucket.replace(' ', 'T') + 'Z'
    return {
      time: new Date(utcStr).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      Fraud: t.fraud_count,
      Legit: t.total - t.fraud_count,
    }
  })

  const fraudRateTimelineData = timeline.map(t => {
    const utcStr = t.hour_bucket.includes('T') ? t.hour_bucket : t.hour_bucket.replace(' ', 'T') + 'Z'
    const rate = t.total > 0 ? (t.fraud_count / t.total) * 100 : 0.0
    return {
      time: new Date(utcStr).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      'Fraud Rate': rate,
    }
  })

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.85rem', flex: 1, height: '100%', minHeight: 0 }}>
      {/* KPI row */}
      <div className="kpi-row" style={{ flexShrink: 0 }}>
        <div className="kpi-mini blue">
          <span className="km-label">Total Scored</span>
          <span className="km-value" style={{ color: 'var(--accent-blue)' }}>{fmt(total)}</span>
        </div>
        <div className="kpi-mini fraud">
          <span className="km-label">Fraud</span>
          <span className="km-value" style={{ color: 'var(--fraud-color)' }}>{fmt(fraud_count)}</span>
        </div>
        <div className="kpi-mini legit">
          <span className="km-label">Legit</span>
          <span className="km-value" style={{ color: 'var(--legit-color)' }}>{fmt(legit_count)}</span>
        </div>
      </div>

      {/* Gauge + chart */}
      <div style={{ display: 'flex', gap: '0.75rem', flex: 1, minHeight: 0 }}>
        <div className="chart-box" style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
          <p className="chart-box-title">Volume — Last {windowLabel}</p>

          {chartData.length > 0 ? (
            <div style={{ flex: 1, minHeight: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData} margin={{ top: 2, right: 2, left: -24, bottom: 0 }}>
                  <defs>
                    <linearGradient id="gF" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--fraud-color)" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="var(--fraud-color)" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="gL" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--legit-color)" stopOpacity={0.25} />
                      <stop offset="95%" stopColor="var(--legit-color)" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="time" tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
                  <YAxis tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
                  <Tooltip content={<TooltipContent />} />
                  <Area type="monotone" dataKey="Legit" stroke="var(--legit-color)" fill="url(#gL)" strokeWidth={1.5} dot={false} />
                  <Area type="monotone" dataKey="Fraud" stroke="var(--fraud-color)" fill="url(#gF)" strokeWidth={1.5} dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)', fontSize: '0.78rem' }}>
              Chưa có dữ liệu
            </div>
          )}
        </div>
        <div className="chart-box" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '1rem', minWidth: '180px', flexShrink: 0 }}>
          <GaugeSVG value={fraud_rate} />
        </div>
      </div>

      {/* Fraud Rate Timeline */}
      {fraudRateTimelineData.length > 0 && (
        <div className="chart-box" style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
          <p className="chart-box-title" style={{ color: 'var(--fraud-color)' }}>Fraud Rate over Time — Last {windowLabel}</p>
          <div style={{ flex: 1, minHeight: 0 }}>
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={fraudRateTimelineData} margin={{ top: 2, right: 2, left: -24, bottom: 0 }}>
                <defs>
                  <linearGradient id="gFR" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="var(--fraud-color)" stopOpacity={0.25} />
                    <stop offset="95%" stopColor="var(--fraud-color)" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="time" tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
                <YAxis tickFormatter={v => `${v.toFixed(1)}%`} tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
                <Tooltip content={<TooltipContent percent />} />
                <Area type="monotone" dataKey="Fraud Rate" stroke="var(--fraud-color)" fill="url(#gFR)" strokeWidth={1.5} dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  )
}

