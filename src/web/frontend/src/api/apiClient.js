const BASE = `http://${window.location.hostname}:8000`

async function get(path) {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

export const api = {
  health: () => get('/api/health'),
  config: () => get('/api/config'),
  snapshot: (windowMins, bucketStr) => {
    const params = new URLSearchParams()
    if (windowMins) params.append('window', windowMins)
    if (bucketStr) params.append('bucket', bucketStr)
    const q = params.toString() ? `?${params.toString()}` : ''
    return get(`/api/snapshot${q}`)
  },
  models: () => get('/api/models'),
  stats: (modelId) => get(`/api/stats/${modelId}`),
  predictions: (modelId, limit = 50, offset = 0) =>
    get(`/api/predictions/${modelId}?limit=${limit}&offset=${offset}`),
  monitor: (modelId, limit = 50, offset = 0) =>
    get(`/api/monitor/${modelId}?limit=${limit}&offset=${offset}`),
  refresh: async (windowMins, bucketStr) => {
    const params = new URLSearchParams()
    if (windowMins) params.append('window', windowMins)
    if (bucketStr) params.append('bucket', bucketStr)
    const q = params.toString() ? `?${params.toString()}` : ''
    const res = await fetch(`${BASE}/api/refresh${q}`, { method: 'POST' })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json()
  },
}
