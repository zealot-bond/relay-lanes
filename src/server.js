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

function pickLane (tried) {
  const candidates = lanes.liveLanes(45000).filter((l) => l.status !== 'exhausted')
  const fresh = candidates.filter((l) => !tried.has(l.id))
  // Prefer a lane that has not already failed this request; only fall back to a
  // reused lane once every known lane has been tried.
  return fresh[0] || (candidates.length ? candidates[candidates.length - 1] : null)
}

async function dispatch (entry) {
  const tried = new Set()
  let attempt = 0
  let empties = 0
  let last = { kind: 'error', status: 0, ms: 0, data: null }

  while (attempt <= RETRY_LIMIT && empties <= RETRY_LIMIT) {
    if (Date.now() - entry.enqueuedAt > MAX_HOLD_MS) return last

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
      const timer = setTimeout(() => fin({ kind: 'timeout', status: 0, ms: 120000, data: null }), 120000)
      entry.pendingResolve = (v) => { clearTimeout(timer); fin(v) }
      waiting.set(entry.id, entry)
      queue.emit('assign', { entry, laneId: lane.id, attempt })
    })
    waiting.delete(entry.id)
    last = result

    if (result.kind === 'ok') {
      lanes.record(lane.id, 'ok', result.ms || 0)
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
      queue.requeue(entry, { front: true })
      continue
    }

    attempt++

    if (result.kind === 'limited') {
      // This egress IP is spent. Retire the lane so the orchestrator replaces it
      // with a runner holding a fresh bucket, then hand the work back.
      lanes.record(lane.id, 'limited', result.ms || 0)
      lanes.retire(lane.id, 'egress bucket exhausted')
      queue.requeue(entry, { front: true })
      continue
    }

    lanes.record(lane.id, result.kind === 'timeout' ? 'timeout' : 'failed', result.ms || 0)
    queue.requeue(entry, { front: true })
    // Backoff before the next attempt so a struggling upstream is not hammered.
    await sleep(Math.min(2000, 150 * attempt))
  }
  return last
}

export function ensureDispatched (entry) {
  if (dispatched.has(entry)) return
  dispatched.add(entry)
  ;(async () => {
    const result = await dispatch(entry)
    queue.complete(entry.id, result)
  })()
}

export function acceptLaneResult (laneId, entryId, result) {
  lanes.heartbeat(laneId)
  const entry = waiting.get(entryId)
  if (!entry) return false
  waiting.delete(entryId)
  if (typeof entry.pendingResolve === 'function') { entry.pendingResolve(result); return true }
  queue.complete(entryId, result)
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
    },
  }
  const queued = queue.push(entry)
  if (!queued.ok) {
    return json(res, 503, { error: { message: 'relay queue is full', type: 'overloaded' } },
      { 'Retry-After': '5' })
  }
  ensureDispatched(entry)
  const result = await new Promise((resolve) => { entry.resolve = resolve })

  if (result.kind !== 'ok') {
    // Upstream is unusable for this request. That is NOT a client rate limit, so
    // it must not be reported as 429.
    return json(res, 502, {
      error: {
        message: `no lane could serve this request (${result.kind}); it was retried ${RETRY_LIMIT}x`,
        type: 'upstream_unavailable',
      },
    }, { 'Retry-After': '5' })
  }

  const completion = toChatCompletion(result.data, model)

  if (!stream) return json(res, 200, completion)

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  })
  const { id, created } = completion
  const send = (delta, finish = null) => {
    res.write(`data: ${JSON.stringify({
      id, object: 'chat.completion.chunk', created, model,
      choices: [{ index: 0, delta, finish_reason: finish }],
    })}\n\n`)
  }
  send({ role: 'assistant', content: '' })
  send({ content: completion.choices[0]?.message?.content ?? '' })
  send({}, 'stop')
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