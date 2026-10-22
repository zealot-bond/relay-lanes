// The live model catalogue: the gateway's list, filtered by a ping, served as github/<id>.
//
// A model is listed only if it answers a one-word ping. That is the whole filter. The gateway
// lists paid models that refuse this relay (401 Missing API key), withdrawn ones (410/404) and
// ones in an outage (503). None of them answer a ping, so none of them are served.
//
// Each model that answers is then pinged once per reasoning effort. The efforts it accepts are
// served as github/<id>:<effort> variants, and its default effort is the highest one it accepts.
// A request for the bare github/<id> uses that default unless the client names an effort. The
// gateway rejects some efforts per model (for example beam-free refuses none and minimal), so
// the set is measured, not assumed.
//
// The list refreshes when the process starts. A relay generation starts every RUN_FOR_S
// (about 3h50m), so that is the refresh cadence. There is no timer. If a refresh fails, it is
// retried every RETRY_MS until one succeeds, so a gateway blip at start cannot leave the relay
// with nothing.
//
// A refresh that fails, or whose pings are inconclusive, keeps the last good list. Pings that
// come back rate-limited mean this egress has no quota to test with, so they are not evidence
// that a model is dead: a model already listed keeps its list and its efforts, and a new one
// is not added.
//
// MODELS in models.js is served only until the first verified refresh, so the relay can answer
// while the pings run. Seed models carry no effort data, so they are served without variants.

import { config } from './config.js'
import { zenBase, headers, newSessionId, newRequestId } from './fingerprint.js'
import { callUpstream } from './lane.js'
import { MODELS as SEED, PROVIDER, splitModelId } from './models.js'

// Highest first. The default for a model is the first one of these it accepts.
export const EFFORT_ORDER = ['xhigh', 'max', 'high', 'medium', 'low', 'minimal', 'none']

const DEFAULT_CONTEXT = 200000
const PING_CONCURRENCY = 6
const PING_TIMEOUT_MS = 60000
const RETRY_MS = 60000
const PING_PROMPT = 'Reply with the single word: pong'

const seedContext = new Map(SEED.map((m) => [m.id, m.contextWindow]))
let entries = new Map(SEED.map((m) => [m.id, { contextWindow: m.contextWindow, efforts: [], defaultEffort: null }]))
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

/** Bare model lookup. Accepts "github/x", "x". Efforts are not considered. */
export function find (modelId) {
  const { model } = splitModelId(modelId)
  const entry = entries.get(model)
  return entry ? { id: model, ...entry } : null
}

/**
 * Resolve a requested id, which may carry an effort: "github/x" or "github/x:high".
 * Returns { id, effort, entry } or null. An effort the model does not accept is null, which
 * the caller reports as an unknown model.
 */
export function resolve (publicId) {
  const { model } = splitModelId(publicId)
  const i = model.lastIndexOf(':')
  const id = i > 0 ? model.slice(0, i) : model
  const effort = i > 0 ? model.slice(i + 1) : null
  const entry = entries.get(id)
  if (!entry) return null
  if (effort !== null && !entry.efforts.includes(effort)) return null
  return { id, effort, entry }
}

export function payload (build) {
  const meta = build ? { build } : {}
  const data = []
  for (const [id, e] of entries) {
    const base = `${PROVIDER}/${id}`
    data.push({
      id: base,
      object: 'model',
      created: 0,
      owned_by: PROVIDER,
      context_window: e.contextWindow,
      default_reasoning_effort: e.defaultEffort,
      reasoning_efforts: e.efforts,
    })
    for (const effort of e.efforts) {
      data.push({
        id: `${base}:${effort}`,
        object: 'model',
        created: 0,
        owned_by: PROVIDER,
        context_window: e.contextWindow,
        reasoning_effort: effort,
      })
    }
  }
  return { object: 'list', ...meta, data }
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

// One short completion with the given reasoning effort (or the gateway's default when omitted).
// Returns the result kind: 'ok' means the model answered with that effort.
export async function defaultPing (id, effort) {
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
        reasoningEffort: effort || undefined,
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

async function pool (items, fn) {
  const out = new Map()
  let next = 0
  const workers = Array.from({ length: Math.min(PING_CONCURRENCY, items.length) }, async () => {
    while (next < items.length) {
      const item = items[next++]
      out.set(item.key, await fn(item))
    }
  })
  await Promise.all(workers)
  return out
}

/**
 * Fetch the gateway's list, ping every model, ping each answering model at every effort, and
 * replace the catalogue with the result. Returns { ok, count, added, removed }. On failure the
 * previous list is kept.
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

  const base = await pool(ids.map((id) => ({ key: id, id })), ({ id }) => ping(id, null))
  const tally = {}
  for (const k of base.values()) tally[k] = (tally[k] || 0) + 1

  const limited = tally.limited || 0
  if (limited * 2 > ids.length) {
    return fail(`${limited} of ${ids.length} pings were rate-limited; this egress has no quota to test with`)
  }
  const answering = ids.filter((id) => base.get(id) === 'ok')
  const kept = ids.filter((id) => base.get(id) === 'limited' && entries.has(id))
  if (!answering.length && !kept.length) return fail(`none of ${ids.length} models answered a ping`)

  // Efforts, for the models that answered. A model kept only because it was rate-limited keeps
  // the efforts it had.
  const probes = answering.flatMap((id) => EFFORT_ORDER.map((effort) => ({ key: `${id}|${effort}`, id, effort })))
  const effortKinds = await pool(probes, ({ id, effort }) => ping(id, effort))
  const effortTally = { probed: probes.length, accepted: 0 }

  const next = new Map()
  for (const id of [...answering, ...kept]) {
    const previous = entries.get(id)
    const efforts = kept.includes(id) && !answering.includes(id)
      ? previous?.efforts || []
      : EFFORT_ORDER.filter((effort) => effortKinds.get(`${id}|${effort}`) === 'ok')
    effortTally.accepted += efforts.length
    next.set(id, {
      contextWindow: seedContext.get(id) ?? previous?.contextWindow ?? DEFAULT_CONTEXT,
      efforts,
      defaultEffort: efforts[0] ?? null,
    })
  }
  // Keep the gateway's order, not the order the pings finished in.
  const ordered = new Map(ids.filter((id) => next.has(id)).map((id) => [id, next.get(id)]))

  const added = [...ordered.keys()].filter((id) => !entries.has(id))
  const removed = [...entries.keys()].filter((id) => !ordered.has(id))
  const first = state.source !== 'live'
  entries = ordered
  state = {
    ...state,
    source: 'live',
    lastSuccessAt: state.lastRefreshAt,
    lastError: null,
    lastChange: { added, removed },
    lastPing: { tested: ids.length, answering: answering.length, byKind: tally, efforts: effortTally },
  }
  const refused = Object.entries(tally).filter(([k]) => k !== 'ok').map(([k, n]) => `${k} ${n}`).join(', ')
  console.log(`[catalogue] ${first ? 'loaded' : 'refreshed'}: ${ordered.size} of ${ids.length} listed models answered a ping` +
    (refused ? ` (not answering: ${refused})` : '') +
    `; ${effortTally.accepted} effort variants across ${ordered.size} models` +
    (added.length ? `; added ${added.join(', ')}` : '') +
    (removed.length ? `; removed ${removed.join(', ')}` : ''))
  return { ok: true, count: ordered.size, added, removed }
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
