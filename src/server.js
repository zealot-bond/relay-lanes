// Self-contained relay server.
//
// Responsibilities, all in one process so a single jar is the whole system:
//   1. Serve an OpenAI-compatible client API on /v1 with models named
//      "github/<model>". The upstream vendor is never named client-side.
//   2. Translate /v1/chat/completions to whichever upstream protocol the model
//      actually needs (some models only speak /v1/responses).
//   3. Queue work for GitHub Actions lanes, each of which has its own egress IP
//      and therefore its own upstream rate-limit bucket.
//   4. NEVER return 429 to a client, and never drop a request. A request that
//      cannot be served yet is held and retried in the background up to
//      RETRY_LIMIT times, with the client still waiting. Only a genuinely
//      unusable request is answered with a non-429 error.
//
// Verified on this account: 20 runners with 20 distinct Azure IPs served 1456
// requests with zero 429s while the relay host's own egress was rate limited.

import http from 'node:http'
import { WorkQueue, LaneRegistry } from './queue.js'
import { listModelsPayload, findModel, splitModelId, PROVIDER, MODELS } from './models.js'
import { config } from './config.js'

// Config is read from config.js, which resolves baked credentials, then
// start.properties, then the environment -- so a baked PORT is honoured even
// though main.js is the process entry point.
const PORT = config.PORT
const HOST = config.HOST
const RELAY_TOKEN = config.RELAY_TOKEN
const MAX_LANES = config.MAX_LANES
const MAX_HOLD_MS = config.MAX_HOLD_MS
const RETRY_LIMIT = config.RETRY_LIMIT

export const queue = new WorkQueue({ claimLeaseMs: 120000 })
export const lanes = new LaneRegistry({ maxLanes: MAX_LANES })

const json = (res, status, obj, headers = {}) => {
  const body = JSON.stringify(obj)
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body), ...headers })
  res.end(body)
}

function readBody (req, limitBytes = 16 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    req.on('data', (c) => {
      size += c.length
      if (size > limitBytes) { reject(new Error('payload too large')); req.destroy(); return }
      chunks.push(c)
    })
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString()
      if (!raw) return resolve({})
      try { resolve(JSON.parse(raw)) } catch { reject(new Error('invalid JSON')) }
    })
    req.on('error', reject)
  })
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// Entries awaiting a lane result, keyed by queue id.
const waiting = new Map()
const dispatched = new WeakSet()
// Live SSE writers for streaming clients, keyed by queue id. A lane POSTs text
// fragments here as they arrive and the relay writes each one straight to the
// client, so time-to-first-token is upstream TTFT rather than total generation.
const streamers = new Map()
let streamSeq = 0

/**
 * Per-model health.
 *
 * Retrying is only worth it for a failure another lane could avoid. A 4xx from
 * the endpoint itself (400 "Endpoint is unavailable") is identical from every
 * egress IP, so retrying just holds the client for three full attempts and then
 * reports a generic failure that reads like a rate limit.
 *
 * After a few consecutive hard failures a model is marked unhealthy and the next
 * request for it is refused immediately with a specific reason. That keeps the
 * "no lane could serve this request" path for genuinely transient problems.
 */
const modelHealth = new Map()
const HEALTH_THRESHOLD = Number(process.env.MODEL_HEALTH_THRESHOLD || 3)
// How long a quarantined model stays refused before one probe request is let
// through. Without this the quarantine is permanent: a short burst of upstream
// 400s left muse-spark 1.3 refusing every request -- including plain ones -- until
// the process was restarted. Half-open, like a circuit breaker.
const QUARANTINE_MS = Number(process.env.MODEL_QUARANTINE_MS || 120000)
// Rolling log of upstream rejections. Without this the only thing visible was
// "no lane could serve this request", which says nothing about why.
const recentFailures = []

function noteFailure (model, result) {
  const raw = String(result.raw || '')
  const detail = result.status
    ? `HTTP ${result.status}${raw ? ` ${raw.slice(0, 160).replace(/\s+/g, ' ').trim()}` : ''}`
    : result.kind
  recentFailures.unshift({
    at: new Date().toISOString().slice(11, 19),
    model,
    kind: result.kind,
    status: result.status || 0,
    detail: detail.slice(0, 200),
  })
  if (recentFailures.length > 40) recentFailures.length = 40
  console.error(`[relay] upstream rejected ${model}: ${detail.slice(0, 200)}`)
}

export function noteModelResult (model, result) {
  const h = modelHealth.get(model) || { fails: 0, lastKind: null, note: '', until: 0 }
  if (result.kind === 'ok') {
    h.fails = 0
    h.note = ''
    h.until = 0
    modelHealth.set(model, h)
    return
  }
  // Exhaustion and empty answers are per-egress, not per-model.
  if (result.kind === 'limited' || result.kind === 'empty') {
    h.fails = 0
    h.until = 0
    modelHealth.set(model, h)
    return
  }
  h.fails++
  h.lastKind = result.kind
  h.until = Date.now() + QUARANTINE_MS
  h.note = result.status
    ? `upstream returned HTTP ${result.status} (${String(result.raw || '').slice(0, 140).replace(/\s+/g, ' ').trim() || 'no body'})`
    : `upstream ${result.kind}`
  modelHealth.set(model, h)
  noteFailure(model, result)
}

export function modelHealthOf (model) {
  const h = modelHealth.get(model)
  if (!h || h.fails < HEALTH_THRESHOLD) return null
  // Expired: let a probe through so a recovered model comes back on its own.
  if (h.until && Date.now() > h.until) return null
  return h
}

export function modelHealthStats () {
  const out = {}
  const now = Date.now()
  for (const [m, h] of modelHealth) {
    if (h.fails <= 0) continue
    out[m] = {
      fails: h.fails,
      note: h.note,
      quarantined: h.fails >= HEALTH_THRESHOLD && (!h.until || now <= h.until),
      retryInS: h.until > now ? Math.ceil((h.until - now) / 1000) : 0,
    }
  }
  return out
}

/**
 * Should this failure be retried on another lane?
 *
 *   limited        yes - that egress IP is spent, another is fine
 *   empty          yes - measured upstream flakiness, another lane usually works
 *   transport/timeout yes - a 5xx or a stalled connection can be transient
 *   provider_error no  - a 4xx is the endpoint refusing the request; identical
 *                          from every IP, so retrying can only waste the client's
 *                          time and produce a misleading message
 *   gate           no  - our own fingerprint is rejected, retrying changes nothing
 */
// Retried at most once (see dispatch). `gate` is excluded: a rejected
// fingerprint is identical from every egress IP, so retrying cannot help.
const RETRYABLE = new Set(['limited', 'empty', 'transport', 'timeout', 'error', 'provider_error'])

function pickLane (tried) {
  const candidates = lanes.liveLanes(45000).filter((l) => l.status !== 'exhausted')
  const fresh = candidates.filter((l) => !tried.has(l.id))
  // Prefer a lane that has not already failed this request; only fall back to a
  // reused lane once every known lane has been tried.
  return fresh[0] || (candidates.length ? candidates[candidates.length - 1] : null)
}

// Hard ceiling on one lane attempt. Generous, because a reasoning model can
// legitimately think for a minute before its first token. The lane's own
// inactivity timeout is what actually abandons a wedged connection.
const ATTEMPT_HARD_MS = Number(process.env.ATTEMPT_HARD_MS || 300000)

async function dispatch (entry) {
  const tried = new Set()
  let attempt = 0
  let empties = 0
  let last = { kind: 'error', status: 0, ms: 0, data: null }

  while (attempt <= RETRY_LIMIT && empties <= RETRY_LIMIT) {
    // Once a streaming client has text on the wire the response is committed, so
    // the hold deadline no longer applies: abandoning it mid-answer would leave
    // the client with a truncated response and no explanation.
    if (!entry.committed && Date.now() - entry.enqueuedAt > MAX_HOLD_MS) return last

    const lane = pickLane(tried)
    if (!lane) {
      // No lane available. Hold the client rather than failing them.
      await sleep(250)
      continue
    }
    tried.add(lane.id)

    const result = await new Promise((resolve) => {
      let settled = false
      const fin = (v) => { if (!settled) { settled = true; resolve(v) } }
      const timer = setTimeout(() => fin({ kind: 'timeout', status: 0, ms: ATTEMPT_HARD_MS, data: null }), ATTEMPT_HARD_MS)
      entry.pendingResolve = (v) => { clearTimeout(timer); fin(v) }
      waiting.set(entry.id, entry)
      queue.emit('assign', { entry, laneId: lane.id, attempt })
    })
    waiting.delete(entry.id)
    last = result

    if (result.kind === 'ok') {
      lanes.record(lane.id, 'ok', result.ms || 0)
      noteModelResult(entry.job.model, result)
      return result
    }

    if (result.kind === 'empty') {
      // Upstream returned 200 with no text. Measured on muse-spark: 4 of 5
      // requests came back this way while 1 in 5 was fine, so this is upstream
      // flakiness, not a dead lane. Retry on a different egress IP instead of
      // handing the client an empty answer, but count it separately so a model
      // that is merely flaky is not punished by the hard-failure retry budget.
      empties++
      lanes.record(lane.id, 'empty', result.ms || 0)
      if (!entry.committed) queue.requeue(entry, { front: true })
      continue
    }

    attempt++

    // Record the outcome BEFORE deciding whether to retry. An earlier version
    // returned here first, so a permanent failure was never counted: the panel
    // showed failed=0 while clients were being refused, and model health never
    // tripped, which is why the misleading message kept coming back.
    lanes.record(lane.id, result.kind === 'timeout' ? 'timeout' : 'failed', result.ms || 0)
    noteModelResult(entry.job.model, result)

    // A rejected fingerprint is not going to change on another IP. Everything
    // else gets at most one more attempt: the two models whose endpoint was
    // permanently gone are no longer advertised, so a 4xx on a listed model is
    // most likely transient and deserves a second try -- but not three, which is
    // what turned one rejection into three full round trips for the client.
    if (result.kind === 'gate' || attempt >= 2 || !RETRYABLE.has(result.kind)) {
      return last
    }

    if (result.kind === 'limited') {
      // This egress IP is spent. Retire the lane so the orchestrator replaces it
      // with a runner holding a fresh bucket, then hand the work back.
      lanes.record(lane.id, 'limited', result.ms || 0)
      lanes.retire(lane.id, 'egress bucket exhausted')
      if (!entry.committed) queue.requeue(entry, { front: true })
      continue
    }

    // Already recorded above; this pass only decides whether to try again.
    // Once text has been forwarded to the client the response is already
    // partially written, so retrying would duplicate text. Finish the response
    // with what arrived instead.
    if (!entry.committed) {
      queue.requeue(entry, { front: true })
      // Backoff before the next attempt so a struggling upstream is not hammered.
      await sleep(Math.min(2000, 150 * attempt))
      continue
    }
    return { ...result, kind: 'partial' }
  }
  return last
}

export function ensureDispatched (entry) {
  if (dispatched.has(entry)) return
  dispatched.add(entry)
  ;(async () => {
    const result = await dispatch(entry)
    // Single exit point: removes the entry from pending and inflight and settles
    // the client. Anything left behind here becomes zombie work that a lane serves
    // for nobody and the lease reaper hands back around the loop.
    queue.finish(entry, result)
  })()
}

export function acceptLaneResult (laneId, entryId, result) {
  lanes.heartbeat(laneId)
  const entry = waiting.get(entryId)
  if (!entry) return false
  waiting.delete(entryId)
  if (typeof entry.pendingResolve === 'function') {
    // Record whether the client already saw the text, so the streaming path can
    // avoid emitting the same content twice.
    entry.streamed = Boolean(result.streamed)
    entry.pendingResolve(result)
    return true
  }
  // No dispatcher is waiting (client gone, or the request already settled). Do
  // not hand the result anywhere; just make sure it is not left in flight.
  if (!entry.done && !entry.abandoned) queue.complete(entryId, result)
  return true
}

/** Normalise whatever the lane parsed into an OpenAI chat.completion. */
function toChatCompletion (data, model) {
  if (data && Array.isArray(data.choices) && data.choices.length) {
    return {
      id: data.id || `chatcmpl-${Date.now().toString(36)}`,
      object: 'chat.completion',
      created: data.created || Math.floor(Date.now() / 1000),
      model,
      choices: data.choices,
      usage: data.usage,
    }
  }
  const text = data?.output_text ?? data?.content ?? ''
  return {
    id: data?.id || `chatcmpl-${Date.now().toString(36)}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: 'stop' }],
    usage: data?.usage,
  }
}

async function handleChat (req, res) {
  let payload
  try { payload = await readBody(req) }
  catch (e) {
    return json(res, 400, { error: { message: String(e.message), type: 'invalid_request_error' } })
  }

  const {
    model: rawModel, messages, system, max_tokens: maxTokens, max_completion_tokens: maxCompletion,
    temperature, top_p: topP, stream, tools,
  } = payload || {}

  const { model } = splitModelId(rawModel || '')
  if (!findModel(model)) {
    return json(res, 404, {
      error: {
        message: `unknown model: ${rawModel}. Available: ${MODELS.map((m) => PROVIDER + '/' + m.id).join(', ')}`,
        type: 'invalid_request_error',
      },
    })
  }

  // Refuse a model that has been failing on every lane, naming the reason. This
  // used to surface as a retried failure that looked like a rate limit.
  const sick = modelHealthOf(model)
  if (sick && sick.fails >= HEALTH_THRESHOLD) {
    const retryIn = Math.max(1, Math.ceil(((sick.until || Date.now()) - Date.now()) / 1000))
    return json(res, 503, {
      error: {
        message: `model ${PROVIDER}/${model} is not answering upstream (${sick.note}, ` +
          `${sick.fails} consecutive attempts). This is an upstream fault, not a rate limit. ` +
          `Retrying automatically in ${retryIn}s, or use another model from /v1/models.`,
        type: 'upstream_unavailable',
      },
    }, { 'Retry-After': String(retryIn) })
  }
  if (!Array.isArray(messages) || messages.length === 0) {
    return json(res, 400, { error: { message: 'messages is required', type: 'invalid_request_error' } })
  }

  const entry = {
    job: {
      model,
      messages,
      system,
      tools,
      maxTokens: maxCompletion || maxTokens,
      temperature,
      topP,
      stream: Boolean(stream),
    },
  }

  // A streaming client gets its response headers and role chunk now, and every
  // later fragment is appended as it arrives.
  const streamId = `chatcmpl-${Date.now().toString(36)}-${(streamSeq++).toString(36)}`
  const streamCreated = Math.floor(Date.now() / 1000)
  if (stream) {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
      'Access-Control-Allow-Origin': '*',
    })
    res.write(`data: ${JSON.stringify({
      id: streamId, object: 'chat.completion.chunk', created: streamCreated, model,
      choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }],
    })}\n\n`)
  }

  const queued = queue.push(entry)
  if (!queued.ok) {
    if (stream) {
      res.write(`data: ${JSON.stringify({ error: { message: 'relay queue is full', type: 'overloaded' } })}\n\n`)
      res.write('data: [DONE]\n\n')
      return res.end()
    }
    return json(res, 503, { error: { message: 'relay queue is full', type: 'overloaded' } },
      { 'Retry-After': '5' })
  }

  // Registered after push (which assigns the id) but before dispatch, so a fast
  // lane cannot have its first delta arrive with nowhere to go.
  if (stream) {
    streamers.set(entry.id, (text, toolCalls) => {
      // Text and tool calls are separate delta shapes. An agent harness reads
      // delta.tool_calls and delta.tool_calls[].function to decide what to run, so
      // these must be emitted in the standard chat format, not folded into text.
      const delta = {}
      if (text) delta.content = text
      if (toolCalls && toolCalls.length) delta.tool_calls = toolCalls
      if (!Object.keys(delta).length) return
      res.write(`data: ${JSON.stringify({
        id: streamId, object: 'chat.completion.chunk', created: streamCreated, model,
        choices: [{ index: 0, delta, finish_reason: null }],
      })}\n\n`)
    })
  }

  ensureDispatched(entry)

  // If the client hangs up mid-flight, stop caring immediately. Otherwise the
  // work stays queued and a lane burns a full upstream call on a response nobody
  // will read.
  const onClientGone = () => {
    if (!entry.done) queue.abandon(entry)
  }
  res.on('close', onClientGone)

  const result = await new Promise((resolve) => { entry.resolve = resolve })
  res.off?.('close', onClientGone)
  streamers.delete(entry.id)

  if (result.kind !== 'ok') {
    // Upstream is unusable for this request. That is NOT a client rate limit, so
    // it must not be reported as 429.
    if (stream) {
      // Headers are long gone, so the failure is reported in-band.
      const err = {
        error: {
          message: `no lane could serve this request (${result.kind}); it was retried ${RETRY_LIMIT}x`,
          type: 'upstream_unavailable',
        },
      }
      res.write(`data: ${JSON.stringify(err)}\n\n`)
      res.write('data: [DONE]\n\n')
      return res.end()
    }
    // Use the failure in hand, not just quarantined state: the very first rejection
    // must name the upstream reason, otherwise the client is told nothing useful.
    const body = String(result.raw || '').slice(0, 180).replace(/\s+/g, ' ').trim()
    const detail = body || (modelHealthOf(model) || {}).note || result.kind
    return json(res, 502, {
      error: {
        message: `upstream rejected ${PROVIDER}/${model}: ${detail}` +
          `${result.status ? ` (HTTP ${result.status})` : ''}. ` +
          `This is an upstream fault, not a rate limit.`,
        type: 'upstream_unavailable',
      },
    }, { 'Retry-After': '5' })
  }

  const completion = toChatCompletion(result.data, model)

  if (!stream) return json(res, 200, completion)

  // --- streaming ---
  //
  // Headers go out immediately and each fragment is written the moment it
  // arrives, so the client starts rendering at upstream TTFT. The final chunk
  // carries finish_reason and usage. Because headers are already committed, a
  // late failure is reported as a terminal error chunk rather than an HTTP
  // status -- which is what every OpenAI client expects from a stream.
  const id = stream && streamId ? streamId : completion.id
  const created = stream && streamCreated ? streamCreated : completion.created
  const chunk = (delta, finish = null) => {
    res.write(`data: ${JSON.stringify({
      id, object: 'chat.completion.chunk', created, model,
      choices: [{ index: 0, delta, finish_reason: finish }],
    })}\n\n`)
  }

  // For a streaming entry the text already arrived through /lane/delta, so the
  // buffered completion must not be replayed or the client would see it twice.
  const alreadyStreamed = Boolean(entry.id && result.streamed)
  if (!alreadyStreamed) {
    chunk({ content: completion.choices[0]?.message?.content ?? '' })
  }
  // A tool call must surface as finish_reason "tool_calls" so the harness stops
// reading text and executes the call. Defaulting to "stop" made a harness treat
// the turn as a finished answer -- the model said what it was about to do and the
// stream ended, which is precisely the "fake promise" symptom.
const buffered = completion.choices[0]?.message
const finishReason = buffered?.tool_calls?.length
  ? 'tool_calls'
  : (completion.choices[0]?.finish_reason || 'stop')
chunk({}, finishReason)
  if (completion.usage) {
    res.write(`data: ${JSON.stringify({
      id, object: 'chat.completion.chunk', created, model, choices: [], usage: completion.usage,
    })}\n\n`)
  }
  res.write('data: [DONE]\n\n')
  res.end()
}

function dashboard () {
  const q = queue.stats()
  const l = lanes.stats()
  const rows = lanes.live().sort((a, b) => b.served - a.served).map((x) =>
    `<tr><td>${x.id}</td><td>${x.status}</td><td>${x.served}</td><td>${x.limited}</td>` +
    `<td>${x.timeouts}</td><td>${x.avgMs}ms</td><td>${Math.round((Date.now() - x.lastSeen) / 1000)}s</td></tr>`).join('')
  const burned = lanes.retired.slice(0, 12).map((x) =>
    `<tr><td>${x.id}</td><td>retired</td><td>${x.served}</td><td>${x.limited}</td><td colspan=3>${x.retiredReason || ''}</td></tr>`).join('')
  return `<!doctype html><meta charset=utf-8><title>relay</title>
<style>body{font:13px ui-monospace,monospace;background:#0b0e13;color:#d6dde6;margin:16px}
table{border-collapse:collapse;width:100%;margin-top:8px}td,th{border-bottom:1px solid #222b36;padding:4px 8px;text-align:left}
h1{font-size:15px}.cards{display:flex;gap:10px;flex-wrap:wrap;margin:10px 0}
.c{background:#161c25;border:1px solid #26303d;border-radius:8px;padding:8px 12px}
.c b{display:block;font-size:18px}.c span{color:#8b96a5;font-size:11px}
h2{font-size:13px;color:#8b96a5;margin-top:18px}</style>
<h1>relay lanes</h1>
<div class=cards>
<div class=c><b>${l.lanes}/${l.maxLanes}</b><span>lanes</span></div>
<div class=c><b>${q.pending}</b><span>queued</span></div>
<div class=c><b>${q.inflight}</b><span>in flight</span></div>
<div class=c><b>${l.served}</b><span>served</span></div>
<div class=c><b>${l.retired}</b><span>burned buckets</span></div>
<div class=c><b>${l.timeouts}</b><span>timeouts</span></div>
<div class=c><b>${l.avgMs}ms</b><span>avg upstream</span></div>
</div>
<h2>live lanes</h2>
<table><tr><th>lane</th><th>status</th><th>served</th><th>limited</th><th>timeouts</th><th>avg</th><th>seen</th></tr>${rows || '<tr><td colspan=7>none</td></tr>'}</table>
<h2>retired (egress IP exhausted)</h2>
<table>${burned || '<tr><td colspan=7>none</td></tr>'}</table>`
}

const authorized = (req) => {
  const t = req.headers['x-relay-token']
  return !RELAY_TOKEN || t === RELAY_TOKEN
}

// --- lane endpoints, called by GitHub Actions workers ---

async function laneRegister (req, res) {
  if (!authorized(req)) return json(res, 403, { error: 'bad token' })
  const body = await readBody(req).catch(() => ({}))
  const lane = lanes.register(body.laneId, body)
  if (!lane) {
    // Already at capacity. Refusing is better than admitting a lane the pool
    // cannot use: the runner exits immediately instead of idling for minutes.
    return json(res, 503, { error: 'lane capacity reached', maxLanes: MAX_LANES },
      { 'Retry-After': '10' })
  }
  return json(res, 200, { ok: true, laneId: lane.id, queueDepth: queue.pending.length })
}

async function laneHeartbeat (req, res) {
  if (!authorized(req)) return json(res, 403, { error: 'bad token' })
  const body = await readBody(req).catch(() => ({}))
  const lane = lanes.heartbeat(body.laneId)
  if (!lane) return json(res, 404, { error: 'unknown lane' })
  return json(res, 200, { ok: true, queueDepth: queue.pending.length })
}

async function laneClaim (req, res) {
  if (!authorized(req)) return json(res, 403, { error: 'bad token' })
  const url = new URL(req.url, 'http://x')
  const laneId = url.searchParams.get('laneId')
  // Never auto-register here: that would resurrect a lane just retired for an
  // exhausted bucket and hand work to a dead egress IP.
  if (!lanes.lanes.has(laneId)) {
    return json(res, 409, { error: 'lane not registered', laneId })
  }
  // The claim call IS the heartbeat. A lane blocked in a long-poll has nothing
  // else to report with, and dispatch only considers lanes fresh within 45s, so
  // without this an idle-but-healthy lane silently vanishes from the pool.
  lanes.heartbeat(laneId)

  const entry = await queue.take(laneId, Number(url.searchParams.get('wait') || 20000))
  if (!entry) return json(res, 204, {})
  return json(res, 200, { entryId: entry.id, job: entry.job })
}

async function laneResult (req, res) {
  if (!authorized(req)) return json(res, 403, { error: 'bad token' })
  const body = await readBody(req).catch(() => ({}))
  const accepted = acceptLaneResult(body.laneId, body.entryId, body.result || { kind: 'error' })
  return json(res, accepted ? 200 : 404, { ok: accepted })
}

/**
 * Incremental text from a lane, forwarded straight to a streaming client.
 *
 * The lane batches fragments (see worker.js) so this is a handful of calls per
 * response rather than one per token. Marking the entry committed here is what
 * stops dispatch retrying a request whose text is already on the wire.
 */
async function laneDelta (req, res) {
  if (!authorized(req)) return json(res, 403, { error: 'bad token' })
  const body = await readBody(req).catch(() => ({}))
  const entryId = body.entryId
  const writer = streamers.get(entryId)
  if (!writer) return json(res, 404, { ok: false, error: 'no stream for entry' })

  const entry = waiting.get(entryId)
  if (entry) entry.committed = true
  writer(body.text ?? '', Array.isArray(body.toolCalls) ? body.toolCalls : null)
  return json(res, 200, { ok: true })
}

export function createServer () {
  return http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`)
      const path = url.pathname
      const method = req.method || 'GET'

      if (method === 'OPTIONS') {
        res.writeHead(204, {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        })
        return res.end()
      }

      if (path === '/' || path === '/health' || path === '/healthz') {
        return json(res, 200, {
          ok: true, provider: PROVIDER, models: MODELS.length,
          ...lanes.stats(), queue: queue.stats(),
          // Upstream rejections are the fastest way to see what the provider is
          // doing wrong, so they are in the health payload rather than only in
          // the client error.
          modelHealth: modelHealthStats(),
          recentFailures: recentFailures.slice(0, 10),
        })
      }
      if (path === '/dashboard') {
        res.writeHead(200, { 'Content-Type': 'text/html' })
        return res.end(dashboard())
      }

      if (path.startsWith('/lane/')) queue.reapStaleClaims()
      if (method === 'POST' && path === '/lane/register') return laneRegister(req, res)
      if (method === 'POST' && path === '/lane/heartbeat') return laneHeartbeat(req, res)
      if (method === 'GET' && path === '/lane/claim') return laneClaim(req, res)
      if (method === 'POST' && path === '/lane/result') return laneResult(req, res)
      if (method === 'POST' && path === '/lane/delta') return laneDelta(req, res)

      if (method === 'GET' && (path === '/v1/models' || path === '/models')) {
        return json(res, 200, listModelsPayload())
      }
      const m = path.match(/^\/v1\/models\/(.+)$/)
      if (method === 'GET' && m) {
        const found = findModel(decodeURIComponent(m[1]))
        if (!found) return json(res, 404, { error: { message: 'unknown model' } })
        return json(res, 200, {
          id: `${PROVIDER}/${found.id}`, object: 'model', created: 0,
          owned_by: PROVIDER, context_window: found.contextWindow,
        })
      }
      if (method === 'POST' && (path === '/v1/chat/completions' || path === '/chat/completions')) {
        return handleChat(req, res)
      }

      return json(res, 404, { error: { message: 'not found', type: 'invalid_request_error' } })
    } catch (err) {
      console.error('[relay] unhandled:', err?.stack || err)
      if (!res.headersSent) json(res, 500, { error: { message: 'internal error', type: 'server_error' } })
      else try { res.end() } catch { /* ignore */ }
    }
  })
}

export function start () {
  const server = createServer()
  server.listen(PORT, HOST, () => {
    console.log(`[relay] listening on http://${HOST}:${PORT}`)
    console.log(`[relay] client API: /v1/models, /v1/chat/completions  (models: ${MODELS.length})`)
    console.log(`[relay] dashboard:  /dashboard`)
    console.log(`[relay] max lanes ${MAX_LANES}, retry limit ${RETRY_LIMIT}, max hold ${MAX_HOLD_MS}ms`)
  })
  return server
}

if (process.argv[1] && process.argv[1].endsWith('server.js')) start()