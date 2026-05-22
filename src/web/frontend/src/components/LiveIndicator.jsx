export function LiveIndicator({ connected, lastUpdated }) {
  const fmt = lastUpdated
    ? new Date(lastUpdated).toLocaleTimeString()
    : '—'

  return (
    <div className="live-indicator">
      <div className={`live-dot ${connected ? '' : 'offline'}`} />
      <span>{connected ? 'Live' : 'Reconnecting…'}</span>
      {lastUpdated && (
        <span className="last-update">· {fmt}</span>
      )}
    </div>
  )
}
