// The live model catalogue: the gateway's list, filtered by a one-word ping, served as github/<id>.
//
// A model is listed only if it answers the ping. That is the whole filter. The gateway lists
// paid models that refuse this relay (401 Missing API key), withdrawn ones (410/404) and ones
// in an outage (503). None of them answer a ping, so none of them are served.
//
// The list refreshes when the process starts. A relay generation starts every RUN_FOR_S
// (about 3h50m), so that is the refresh cadence. There is no timer. If a refresh fails, it is
// retried every RETRY_MS until one succeeds, so a gateway blip at start cannot leave the relay
// with nothing.
//
// A refresh that fails, or whose pings are inconclusive, keeps the last good list. Pings that
// come back rate-limited mean this egress has no quota to test with, so they are not evidence
// that a model is dead: a model already listed stays listed, and a new one is not added.
//
// MODELS in models.js is served only until the first verified refresh, so the relay can answer
// while the pings run.

import { config } from './config.js'
import { zenBase, headers, newSessionId, newRequestId } from './fingerprint.js'
import { callUpstream } from './lane.js'
import { MODELS as SEED, PROVIDER, splitModelId } from './models.js'

const DEFAULT_CONTEXT = 200000
const PING_CONCURRENCY = 6
const PING_TIMEOUT_MS = 60000
const RETRY_MS = 60000
const PING_PROMPT = 'Reply with the single word: pong'

const seedContext = new Map(SEED.map((m) => [m.id, m.contextWindow]))
let entries = new Map(SEED.map((m) => [m.id, m.contextWindow]))
let state = {
  source: 'seed',
  lastRefreshAt: null,
  lastSuccessAt: null,
  lastError: null,
  refreshes: 0,
  lastChange: { added: [], removed: [] },
  lastPing: null,
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
    lastRefreshAt: state.lastRefreshAt,
    lastSuccessAt: state.lastSuccessAt,
    lastError: state.lastError,
    refreshes: state.refreshes,
    lastChange: state.lastChange,
    lastPing: state.lastPing,
  }
}

// One short completion. Returns the result kind: 'ok' means the model answered.
export async function defaultPing (id) {
  let timer
  const timeout = new Promise((resolve) => {
    timer = setTimeout(() => resolve({ kind: 'timeout' }), PING_TIMEOUT_MS)
    timer.unref?.()
  })
  try {
    const r = await Promise.race([
      callUpstream({
        model: id,
        messages: [{ role: 'user', content: PING_PROMPT }],
        stream: true,
        toolChoice: 'none',
        maxTokens: 64,
      }),
      timeout,
    ])
    return r.kind
  } catch {
    return 'throw'
  } finally {
    clearTimeout(timer)
  }
}

async function pingAll (ids, ping) {
  const kinds = new Map()
  let next = 0
  const workers = Array.from({ length: Math.min(PING_CONCURRENCY, ids.length) }, async () => {
    while (next < ids.length) {
      const id = ids[next++]
      kinds.set(id, await ping(id))
    }
  })
  await Promise.all(workers)
  return kinds
}

/**
 * Fetch the gateway's list, ping every model, and replace the catalogue with the ones that
 * answered. Returns { ok, count, added, removed }. On failure the previous list is kept.
 */
export async function refresh ({ ping = defaultPing } = {}) {
  state.refreshes++
  state.lastRefreshAt = new Date().toISOString()
  const fail = (message) => {
    state.lastError = message
    console.error(`[catalogue] refresh failed, keeping ${entries.size} models: ${message}`)
    return { ok: false, count: entries.size, added: [], removed: [] }
  }

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
    return fail(e.message)
  }

  const kinds = await pingAll(ids, ping)
  const tally = {}
  for (const k of kinds.values()) tally[k] = (tally[k] || 0) + 1
  state.lastPing = { tested: ids.length, answering: tally.ok || 0, byKind: tally }

  const limited = tally.limited || 0
  if (limited * 2 > ids.length) {
    return fail(`${limited} of ${ids.length} pings were rate-limited; this egress has no quota to test with`)
  }
  const alive = ids.filter((id) => kinds.get(id) === 'ok' || (kinds.get(id) === 'limited' && entries.has(id)))
  if (!alive.length) return fail(`none of ${ids.length} models answered a ping`)

  const next = new Map(alive.map((id) => [id, seedContext.get(id) ?? DEFAULT_CONTEXT]))
  const added = alive.filter((id) => !entries.has(id))
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
  const refused = Object.entries(tally).filter(([k]) => k !== 'ok').map(([k, n]) => `${k} ${n}`).join(', ')
  console.log(`[catalogue] ${first ? 'loaded' : 'refreshed'}: ${next.size} of ${ids.length} listed models answered a ping` +
    (refused ? ` (not answering: ${refused})` : '') +
    (added.length ? `; added ${added.join(', ')}` : '') +
    (removed.length ? `; removed ${removed.join(', ')}` : ''))
  return { ok: true, count: next.size, added, removed }
}

/** Refresh now. Keep retrying every RETRY_MS until one refresh succeeds. Never refreshes again. */
export function start () {
  const attempt = () => {
    refresh().then((r) => { if (!r.ok) retry() }, () => retry())
  }
  const retry = () => {
    const t = setTimeout(attempt, RETRY_MS)
    t.unref?.()
  }
  attempt()
}
