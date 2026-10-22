// The live model catalogue: whatever the OpenCode gateway lists right now, served as
// github/<id>. Nothing here is a hard-coded list or a cap. A model the gateway adds appears
// on the next refresh; a model it removes disappears from /v1/models and is refused like any
// unknown id.
//
// MODELS in models.js is only the seed. It is served until the first successful fetch, so
// the relay can answer before it has reached the gateway, and nothing else reads it.
//
// A refresh that fails, or returns something unusable, keeps the last good list. A gateway
// blip must never empty the catalogue, so a failed refresh is logged and the old list stays.

import { config } from './config.js'
import { zenBase, headers, newSessionId, newRequestId } from './fingerprint.js'
import { MODELS as SEED, PROVIDER, splitModelId } from './models.js'

const DEFAULT_CONTEXT = 200000
const seedContext = new Map(SEED.map((m) => [m.id, m.contextWindow]))

let entries = new Map(SEED.map((m) => [m.id, m.contextWindow]))
let state = {
  source: 'seed',
  lastRefreshAt: null,
  lastSuccessAt: null,
  lastError: null,
  refreshes: 0,
  lastChange: { added: [], removed: [] },
}

const listUrl = () => config.CATALOGUE_URL || `${zenBase()}/v1/models`

export function count () { return entries.size }

export function find (modelId) {
  const { model } = splitModelId(modelId)
  if (!entries.has(model)) return null
  return { id: model, contextWindow: entries.get(model) }
}

export function payload (build) {
  const meta = build ? { build } : {}
  return {
    object: 'list',
    ...meta,
    data: [...entries].map(([id, contextWindow]) => ({
      id: `${PROVIDER}/${id}`,
      object: 'model',
      created: 0,
      owned_by: PROVIDER,
      context_window: contextWindow,
    })),
  }
}

export function status () {
  return {
    source: state.source,
    models: entries.size,
    refreshEverySeconds: config.CATALOGUE_REFRESH_S,
    lastRefreshAt: state.lastRefreshAt,
    lastSuccessAt: state.lastSuccessAt,
    lastError: state.lastError,
    refreshes: state.refreshes,
    lastChange: state.lastChange,
  }
}

/**
 * Fetch the gateway's list and replace the catalogue with it.
 * Returns { ok, count, added, removed }. On failure the previous list is kept.
 */
export async function refresh () {
  state.refreshes++
  state.lastRefreshAt = new Date().toISOString()
  let ids
  try {
    const res = await fetch(listUrl(), {
      headers: headers({ sessionId: newSessionId(), requestId: newRequestId(), protocol: 'chat' }),
      signal: AbortSignal.timeout(15000),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const body = await res.json()
    if (!Array.isArray(body?.data)) throw new Error('response has no data array')
    ids = [...new Set(body.data
      .map((m) => (typeof m?.id === 'string' ? m.id.trim() : ''))
      .filter(Boolean))]
    if (!ids.length) throw new Error('gateway listed no models')
  } catch (e) {
    state.lastError = e.message
    console.error(`[catalogue] refresh failed, keeping ${entries.size} models: ${e.message}`)
    return { ok: false, count: entries.size, added: [], removed: [] }
  }

  const next = new Map(ids.map((id) => [id, seedContext.get(id) ?? DEFAULT_CONTEXT]))
  const added = ids.filter((id) => !entries.has(id))
  const removed = [...entries.keys()].filter((id) => !next.has(id))
  const first = state.source !== 'live'
  entries = next
  state = {
    ...state,
    source: 'live',
    lastSuccessAt: state.lastRefreshAt,
    lastError: null,
    lastChange: { added, removed },
  }
  if (first || added.length || removed.length) {
    console.log(`[catalogue] ${first ? 'loaded' : 'refreshed'} ${next.size} models from the gateway` +
      (added.length ? `; added ${added.join(', ')}` : '') +
      (removed.length ? `; removed ${removed.join(', ')}` : ''))
  }
  return { ok: true, count: next.size, added, removed }
}

/** Refresh now, then on a timer. The timer never keeps the process alive. */
export function start () {
  const every = Math.max(60, Number(config.CATALOGUE_REFRESH_S) || 3600) * 1000
  refresh().catch(() => {})
  const timer = setInterval(() => { refresh().catch(() => {}) }, every)
  timer.unref?.()
  return timer
}
