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
import { zenBase, headers, newSessionId, newRequestId, OPENCODE_UA } from './fingerprint.js'
import { callUpstream } from './lane.js'
import { MODELS as SEED, PROVIDER, splitModelId } from './models.js'

// Highest first. The default for a model is the first one of these it accepts.
export const EFFORT_ORDER = ['xhigh', 'max', 'high', 'medium', 'low', 'minimal', 'none']

const DEFAULT_CONTEXT = 200000

// Which models are served at all. The gateway lists dozens of free-suffixed models that
// this deployment has no reason to expose; the operator's choice lives in config, not in a
// hardcoded list here. Empty means "serve every model that answers".
const allow = (() => {
  const raw = String(config.CATALOGUE_ALLOW || '').trim()
  // '*' means no filter. An empty env value cannot express that: config.js ignores empty
  // values so a property can never blank a baked secret.
  if (raw === '*' || raw === '') return new Set()
  return new Set(raw.split(',').map((s) => s.trim()).filter(Boolean))
})()
const allowed = (id) => allow.size === 0 || allow.has(id)

// Measured windows, "id=window,id=window": the operator's own measurement, used when the
// published limit lags reality. Parsed once; anything malformed is ignored.
const windowOverrides = (() => {
  const out = new Map()
  for (const part of String(config.MODEL_CONTEXT_WINDOWS || '').split(',')) {
    const eq = part.indexOf('=')
    if (eq <= 0) continue
    const id = part.slice(0, eq).trim()
    const n = Number.parseInt(part.slice(eq + 1), 10)
    if (id && Number.isFinite(n) && n > 0) out.set(id, n)
  }
  return out
})()

// Published limits from the catalogue the first-party client itself loads
// (models.opencode.ai, falling back to models.dev). The gateway's own list publishes
// neither a context window nor an output limit, so these used to be a hardcoded table --
// which meant a stale claim outlived every provider change and a model could be advertised
// with a window it refuses. The upstream catalogue is the source of truth; the override
// above covers models measured to accept more than they publish.
let published = new Map() // id -> { context, output }
let publishedAt = null
// Windows learned DOWNWARD from a "maximum context length" refusal. A declared window this
// relay has seen refused is never advertised again in this generation: the refusal is
// evidence, the published number is not.
const learnedCeiling = new Map() // id -> advertised window after the refusal

function declaredWindow (id, previous) {
  const base = windowOverrides.get(id) ?? published.get(id)?.context ?? previous?.contextWindow ?? DEFAULT_CONTEXT
  const cap = learnedCeiling.get(id)
  const value = cap ? Math.min(base, cap) : base
  return Math.max(config.MIN_CONTEXT_WINDOW, value)
}

function declaredOutput (id, previous) {
  return published.get(id)?.output ?? previous?.maxOutput ?? null
}

export function capabilityOf (id) {
  return { contextWindow: declaredWindow(id), maxOutput: declaredOutput(id) }
}

/**
 * Record that a model refused a prompt for being too long. The window is halved rather
 * than set to a guess, and it stays lowered for the rest of this generation. Halving
 * converges in a couple of refusals even when the published limit is wrong by 4x, and the
 * client is told to compact on the first one instead of repeating it.
 */
export function noteContextRefusal (id) {
  const entry = entries.get(id)
  if (!entry) return null
  const next = Math.max(config.MIN_CONTEXT_WINDOW, Math.floor(entry.contextWindow / 2))
  if (next >= entry.contextWindow) return entry.contextWindow
  learnedCeiling.set(id, next)
  entries.set(id, { ...entry, contextWindow: next })
  console.log(`[catalogue] ${id}: context window lowered to ${next} after a context-length refusal`)
  return next
}

/** Read the published limits once per refresh. Failure keeps the previous values. */
async function loadPublished () {
  const url = config.MODELS_CATALOGUE_URL
  if (!url || url === 'off' || url === 'none') return
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': OPENCODE_UA, Accept: 'application/json' },
      signal: AbortSignal.timeout(20000),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const body = await res.json()
    const provider = body?.providers?.opencode ?? body?.opencode
    const models = provider?.models
    if (!models || typeof models !== 'object') throw new Error('no opencode provider in the catalogue')
    const next = new Map()
    for (const [id, m] of Object.entries(models)) {
      const context = Number(m?.limit?.context)
      const output = Number(m?.limit?.output)
      if (Number.isFinite(context) && context > 0) {
        next.set(id, { context, output: Number.isFinite(output) && output > 0 ? output : null })
      }
    }
    if (!next.size) throw new Error('catalogue published no limits')
    published = next
    publishedAt = new Date().toISOString()
  } catch (e) {
    console.error(`[catalogue] published limits unavailable (${e.message}); keeping ${published.size} known windows`)
  }
}

const PING_CONCURRENCY = 6
const PING_TIMEOUT_MS = 60000
const RETRY_MS = 60000
const EFFORT_RETRY_MS = 30000
const PING_PROMPT = 'Reply with the single word: pong'

let entries = new Map(SEED
  .filter((m) => allowed(m.id))
  .map((m) => [m.id, { contextWindow: m.contextWindow, maxOutput: null, efforts: [], defaultEffort: null }]))
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
    const limits = e.maxOutput ? { max_output_tokens: e.maxOutput } : {}
    data.push({
      id: base,
      object: 'model',
      name: id,
      created: 0,
      owned_by: PROVIDER,
      context_window: e.contextWindow,
      ...limits,
      default_reasoning_effort: e.defaultEffort,
      reasoning_efforts: e.efforts,
    })
    for (const effort of e.efforts) {
      data.push({
        id: `${base}:${effort}`,
        object: 'model',
        name: `${id} · ${effort}`,
        created: 0,
        owned_by: PROVIDER,
        context_window: e.contextWindow,
        ...limits,
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
    allow: [...allow],
    publishedAt,
    learnedWindows: Object.fromEntries(learnedCeiling),
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
export async function refresh ({ ping = defaultPing, retryMs = EFFORT_RETRY_MS } = {}) {
  state.refreshes++
  state.lastRefreshAt = new Date().toISOString()
  const fail = (message) => {
    state.lastError = message
    console.error(`[catalogue] refresh failed, keeping ${entries.size} models: ${message}`)
    return { ok: false, count: entries.size, added: [], removed: [] }
  }

  let ids
  let listed = 0
  try {
    const res = await fetch(listUrl(), {
      headers: headers({ sessionId: newSessionId(), requestId: newRequestId(), protocol: 'chat' }),
      signal: AbortSignal.timeout(15000),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const body = await res.json()
    if (!Array.isArray(body?.data)) throw new Error('response has no data array')
    listed = new Set(body.data
      .map((m) => (typeof m?.id === 'string' ? m.id.trim() : ''))
      .filter(Boolean)).size
    ids = [...new Set(body.data
      .map((m) => (typeof m?.id === 'string' ? m.id.trim() : ''))
      .filter(Boolean))]
      .filter(allowed)
    if (!ids.length) {
      throw new Error(allow.size
        ? `gateway listed ${listed} models, none of them in CATALOGUE_ALLOW (${[...allow].join(', ')})`
        : 'gateway listed no models')
    }
  } catch (e) {
    return fail(e.message)
  }

  // The advertised window comes from the catalogue the client itself loads, not from a
  // table in this repository. A failed fetch keeps whatever was known before.
  await loadPublished()

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
  // A probe that was rate-limited or failed says nothing about the effort. It is retried once
  // after a pause, and counted, rather than being read as "this model refuses that effort".
  const unsure = probes.filter((p) => ['limited', 'throw', 'timeout'].includes(effortKinds.get(p.key)))
  if (unsure.length) {
    await new Promise((resolve) => setTimeout(resolve, retryMs))
    for (const [key, kind] of await pool(unsure, ({ id, effort }) => ping(id, effort))) effortKinds.set(key, kind)
  }
  const effortByKind = {}
  for (const kind of effortKinds.values()) effortByKind[kind] = (effortByKind[kind] || 0) + 1
  const effortTally = { probed: probes.length, retried: unsure.length, accepted: 0, byKind: effortByKind }

  const next = new Map()
  for (const id of [...answering, ...kept]) {
    const previous = entries.get(id)
    const efforts = kept.includes(id) && !answering.includes(id)
      ? previous?.efforts || []
      : EFFORT_ORDER.filter((effort) => effortKinds.get(`${id}|${effort}`) === 'ok')
    effortTally.accepted += efforts.length
    next.set(id, {
      contextWindow: declaredWindow(id, previous),
      maxOutput: declaredOutput(id, previous),
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
    lastPing: {
      tested: ids.length,
      listed,
      skipped: Math.max(0, listed - ids.length),
      answering: answering.length,
      byKind: tally,
      efforts: effortTally,
    },
  }
  const refused = Object.entries(tally).filter(([k]) => k !== 'ok').map(([k, n]) => `${k} ${n}`).join(', ')
  console.log(`[catalogue] ${first ? 'loaded' : 'refreshed'}: ${ordered.size} of ${ids.length} probeable models answered a ping` +
    (listed > ids.length ? ` (${listed - ids.length} filtered out by CATALOGUE_ALLOW)` : '') +
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
