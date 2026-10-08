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
import { CODE_VERSION } from './codeversion.js'

// Config is read from config.js, which resolves baked credentials, then
// start.properties, then the environment -- so a baked PORT is honoured even
// though main.js is the process entry point.
const PORT = config.PORT
const HOST = config.HOST
const RELAY_TOKEN = config.RELAY_TOKEN
const MAX_LANES = config.MAX_LANES
const MAX_HOLD_MS = config.MAX_HOLD_MS
const RETRY_LIMIT = config.RETRY_LIMIT
const STREAM_KEEPALIVE_MS = config.STREAM_KEEPALIVE_MS
const MAX_BACKLOG_BYTES = config.MAX_BACKLOG_BYTES
// How long a finished stream may wait for a slow client to read its tail.
const END_DRAIN_TIMEOUT_MS = config.END_DRAIN_TIMEOUT_MS

export const queue = new WorkQueue({ claimLeaseMs: 120000, avoidTtlMs: 60000 })
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

const dispatched = new WeakSet()
// Live SSE writers for streaming clients, keyed by queue id. A lane POSTs text
// fragments here as they arrive and the relay writes each one straight to the
// client, so time-to-first-token is upstream TTFT rather than total generation.
const streamers = new Map()
let streamSeq = 0
// Monotonic per-attempt id. A lane echoes it back on every delta and on its
// result so output from a superseded attempt can be rejected.
let attemptSeq = 0

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
const HEALTH_THRESHOLD = config.MODEL_HEALTH_THRESHOLD
// How long a quarantined model stays refused before one probe request is let
// through. Without this the quarantine is permanent: a short burst of upstream
// 400s left muse-spark 1.3 refusing every request -- including plain ones -- until
// the process was restarted. Half-open, like a circuit breaker.
const QUARANTINE_MS = config.MODEL_QUARANTINE_MS
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

const UNCOUNTED_KINDS = new Set(['ok', 'limited', 'empty', 'transport', 'timeout', 'error'])

/** Record an attempt's rejection in recentFailures without touching the health counter. */
function logAttemptFailure (model, result) {
  if (!UNCOUNTED_KINDS.has(result.kind)) noteFailure(model, result)
}

export function noteModelResult (model, result, { log = true } = {}) {
  const h = modelHealth.get(model) || { fails: 0, lastKind: null, note: '', until: 0 }
  if (result.kind === 'ok') {
    h.fails = 0
    h.note = ''
    h.until = 0
    modelHealth.set(model, h)
    return
  }
  // Per-egress conditions, not per-model: a spent bucket, an empty answer, and a
  // dead connection are all properties of the lane that drew them. Counting them
  // against the model let three consecutive lane-level blips (an upstream 5xx, a
  // reset socket) take a whole model offline for the quarantine window -- even
  // though the same outcomes are treated as retryable on another lane.
  if (result.kind === 'limited' || result.kind === 'empty' ||
      result.kind === 'transport' || result.kind === 'timeout' || result.kind === 'error') {
    h.fails = 0
    h.until = 0
    modelHealth.set(model, h)
    return
  }
  // Client prompt errors (4xx other than genuine outages) do not indicate a broken model;
  // counting them would let a single user sending bad prompts quarantine the model for everyone.
  const isPromptError = result.status >= 400 && result.status < 500 && !result.outage &&
    /invalid_request|invalid prompt|bad_request|context_length/i.test(String(result.raw || ''))
  if (isPromptError) return

  h.fails++
  h.lastKind = result.kind
  h.until = Date.now() + QUARANTINE_MS
  h.note = result.status
    ? `upstream returned HTTP ${result.status} (${String(result.raw || '').slice(0, 140).replace(/\s+/g, ' ').trim() || 'no body'})`
    : `upstream ${result.kind}`
  modelHealth.set(model, h)
  if (log) noteFailure(model, result)
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
const RETRYABLE = new Set(['limited', 'empty', 'transport', 'timeout', 'error'])
// Attempts per request, including the first. Two is deliberate: a 4xx is identical
// from every egress IP, so a second try covers genuine transients and a third only
// holds the client longer before the same answer.
const MAX_ATTEMPTS = 2

// How recently a lane must have been seen to count as live. One value, used by
// lane selection, attempt supervision and the claim reaper: when these drifted
// apart a lane could be "live" for selection and "stale" for the reaper at the
// same moment, which is how work got requeued while a lane was still serving it.
const LANE_STALE_MS = config.LANE_STALE_MS

/**
 * Choose a lane.
 *
 * This used to return `candidates[0]`, which is Map insertion order -- the
 * oldest-registered lane. Six identical requests in a row all went to one lane
 * while the other 19 sat idle, and a lane mid-way through a 200s request was the
 * only reason any other got used at all.
 *
 * Now: lanes that have not yet seen this request are eligible, and among them the
 * least-recently-used wins (the opposite of insertion order), with average
 * observed latency as a tiebreak. A lane this request already failed on is only
 * reused once every other option is exhausted.
 */
/** Breakdown of which lane-side code the live lanes are running. */
export function laneCodeStats () {
  const versions = {}
  let current = 0, stale = 0, unreported = 0
  for (const l of lanes.live()) {
    const v = l.codeVersion
    if (!v) { unreported++; versions.unreported = (versions.unreported || 0) + 1; continue }
    versions[v] = (versions[v] || 0) + 1
    if (v === CODE_VERSION) current++; else stale++
  }
  return { expected: CODE_VERSION, current, stale, unreported, versions }
}

function pickLane (tried, busy = new Set()) {
  // A lane already assigned an attempt is skipped: it serves one entry at a
  // time, so sending a second request at it parks the client behind work another
  // idle lane could start immediately. This is the main cause of tool-turn
  // latency growth -- a tool-heavy request holds a lane for many seconds, and
  // without this every concurrent client queued behind that one busy lane.
  const candidates = lanes.liveLanes(LANE_STALE_MS)
    .filter((l) => l.status !== 'exhausted' && !busy.has(l.id))
  if (!candidates.length) return null

  const fresh = candidates.filter((l) => !tried.has(l.id))
  const pool = fresh.length ? fresh : candidates

  // Fewest observations first (unknown lanes get a trial), then lowest measured
  // latency, then least recently seen, then id: the final tiebreak must be total,
  // or two entries can sort a different "first" out of the same pool and pile onto
  // one lane while the rest idle.
  return pool.slice().sort((a, b) => {
    // Lanes running current code come first. A lane on stale code can corrupt tool
    // calls, so it only gets work when no current lane is available.
    const sa = a.codeVersion === CODE_VERSION ? 0 : 1
    const sb = b.codeVersion === CODE_VERSION ? 0 : 1
    if (sa !== sb) return sa - sb
    if (a.served !== b.served) return a.served - b.served
    if (a.avgMs !== b.avgMs) return a.avgMs - b.avgMs
    if (a.lastSeen !== b.lastSeen) return a.lastSeen - b.lastSeen
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0
  })[0]
}

// Hard ceiling on one lane attempt. Generous, because a reasoning model can
// legitimately think for a minute before its first token. The lane's own
// inactivity timeout is what actually abandons a wedged connection.
const ATTEMPT_HARD_MS = config.ATTEMPT_HARD_MS
// However, an attempt is NOT abandoned merely because this much time passed: a
// long tool turn or a large prefill holds the claim while the lane keeps
// heartbeating. The ceiling renews while that lane is provably alive, up to this
// absolute bound. Without the renewal the relay gave up at 300s and requeued work
// a lane was still actively serving -- two lanes then streamed into one client,
// which is interleaved text and doubled tool-call arguments.
const ATTEMPT_MAX_MS = config.ATTEMPT_MAX_MS
// How long an entry may sit claimed before its holder is judged on liveness. A
// lane that just claimed work has not necessarily heartbeated since, so judging
// it immediately would requeue work that was picked up milliseconds ago.
const CLAIM_GRACE_MS = config.CLAIM_GRACE_MS

// Lanes currently assigned an attempt. The lane itself serves one entry at a
// time, so dispatching a second request at it only parks the client behind work
// that a different idle lane could take now.
const busyLanes = new Set()

/** The lane that actually served an attempt, for attribution and avoidance. */
const servedLaneId = (result, lane) => result.laneId || lane.id

/**
 * Hand a result to the dispatcher, buffering it when the dispatcher is not
 * currently parked on a promise.
 *
 * This is not optional politeness: after a requeue, the dispatcher sleeps before
 * its next attempt, and a fast lane can claim and answer inside that window. The
 * old code looked up a resolver that had already been cleared, dropped the result
 * on the floor, and the client hung forever while the entry sat in `inflight`
 * until the lease expired. Buffering makes the handoff independent of timing.
 */
function deliverResult (entry, result) {
  if (typeof entry.resultWaiter === 'function') {
    const wake = entry.resultWaiter
    entry.resultWaiter = null
    wake(result)
    return
  }
  entry.results.push(result)
}

/** Resolve with the next result for this entry, waiting if none has arrived. */
function nextResult (entry) {
  if (entry.results.length) return Promise.resolve(entry.results.shift())
  return new Promise((resolve) => { entry.resultWaiter = resolve })
}

/** One attempt: wait for the result of whichever lane serves this entry. */
function waitForAttempt (entry, lane) {
  return new Promise((resolve) => {
    let settled = false
    let waited = 0
    let timer = null
    let poll = null
    const fin = (v) => {
      if (settled) return
      settled = true
      if (timer) clearTimeout(timer)
      if (poll) clearInterval(poll)
      // A promise left dangling would swallow the result of the next attempt.
      if (typeof entry.resultWaiter === 'function') entry.resultWaiter = null
      resolve(v)
    }

    // The dispatcher is parked here for the whole attempt, so any result that
    // arrives is delivered through entry.resultWaiter.
    nextResult(entry).then(fin)

    const stale = (laneId) => {
      const rec = laneId ? lanes.lanes.get(laneId) : null
      return !rec || Date.now() - rec.lastSeen > LANE_STALE_MS
    }
    // Check the lane that actually HOLDS the claim, not the one pickLane guessed.
    // The queue hands work to whichever lane claims first, so the hint can be a
    // different lane entirely; requeueing on the hint's staleness would abandon
    // work that another lane was serving correctly.
    const laneGone = () => {
      const held = queue.inflight.get(entry.id)
      if (held && held.claimedAt && Date.now() - held.claimedAt < CLAIM_GRACE_MS) return false
      return stale(held?.claimedBy || lane.id)
    }

    // A lane can exit cleanly (idle timeout, lifetime end, job cancelled) and
    // simply stop claiming. It stays in the registry until it goes stale, so
    // waiting for it burns the full attempt ceiling: measured as a 300s stall
    // reported to the client as "no lane could serve this request". Polling the
    // lane's liveness turns that into a prompt retry on another lane, which is
    // the whole point of having twenty of them.
    poll = setInterval(() => {
      // Only safe while nothing has been streamed: once the client has text, the
      // response is committed and a retry would duplicate it.
      if (!entry.committed && laneGone()) {
        fin({ kind: 'requeued', status: 0, ms: Date.now() - entry.enqueuedAt, data: null, laneId: lane.id })
      }
    }, 2000)
    poll.unref?.()

    const arm = () => {
      timer = setTimeout(() => {
        waited += ATTEMPT_HARD_MS
        const rec = lanes.lanes.get(lane.id)
        const alive = rec && Date.now() - rec.lastSeen < LANE_STALE_MS
        if (alive && waited < ATTEMPT_MAX_MS) return arm()
        fin({ kind: 'timeout', status: 0, ms: waited, data: null, laneId: lane.id })
      }, ATTEMPT_HARD_MS)
      timer.unref?.()
    }
    arm()
  })
}

async function dispatch (entry) {
  // Lanes that already served this request and failed it. The queue consults
  // this when handing out work, so a retry lands on a different egress IP -- a
  // rejection or an exhausted bucket is identical from the IP that just produced
  // it. `tried` mirrors it for the local pickLane hint.
  const tried = entry.avoid instanceof Map ? new Set(entry.avoid.keys()) : new Set()
  entry.results = entry.results || []
  entry.resultWaiter = null
  let attempt = 0
  let empties = 0
  let last = { kind: 'error', status: 0, ms: 0, data: null }

  while (attempt <= RETRY_LIMIT && empties <= RETRY_LIMIT) {
    if (entry.done || entry.abandoned) return { kind: 'abandoned', status: 0, ms: 0, data: null }

    // Once a streaming client has text on the wire the response is committed, so
    // the hold deadline no longer applies: abandoning it mid-answer would leave
    // the client with a truncated response and no explanation. A committed stream
    // still gets an absolute bound, or a lane that keeps dying would hold the
    // socket open forever.
    const held = Date.now() - entry.enqueuedAt
    if (!entry.committed && held > MAX_HOLD_MS) return last
    if (held > MAX_HOLD_MS * 2) return entry.committed ? { ...last, kind: 'partial' } : last

    const lane = pickLane(tried, busyLanes)
    if (!lane) {
      // No lane available. Hold the client rather than failing them.
      await sleep(250)
      continue
    }

    busyLanes.add(lane.id)
    let result
    try {
      result = await waitForAttempt(entry, lane)
    } finally {
      busyLanes.delete(lane.id)
    }

    // The lane vanished mid-claim (exited, or the relay reaped its claim). The
    // entry is either already back in `pending` (reaped) or still held in
    // `inflight` (the lane simply stopped), and it must be made claimable before
    // looping -- otherwise this spins on a claim no lane can ever take. The lane
    // is marked so the retry lands somewhere else, and no attempt is consumed:
    // the request was never actually served.
    if (result.kind === 'requeued') {
      const gone = servedLaneId(result, lane)
      queue.avoidLane(entry, gone)
      tried.add(gone)
      // The reaper has already put the entry back, and a lane waiting in take() can
      // claim it inside that same emit -- before this microtask runs. Calling
      // requeue() then deleted the NEW lane's live claim from `inflight` and queued
      // the entry again while it was being served: the upstream call was wasted,
      // the lane's result was rejected, and the client waited about twice as long.
      // A claim held by a lane other than the one that vanished is live; leave it.
      const liveClaim = queue.inflight.get(entry.id) === entry &&
        entry.claimedBy && entry.claimedBy !== gone
      // Text already on the wire cannot be replayed: finish with what arrived.
      if (entry.committed) return { ...last, kind: 'partial' }
      if (!liveClaim) queue.requeue(entry, { front: true })
      await sleep(200)
      continue
    }
    if (result.kind === 'abandoned' || entry.abandoned) {
      return { kind: 'abandoned', status: 0, ms: 0, data: null }
    }

    // Attribute the outcome to the lane that actually served it, not to the one
    // pickLane guessed: the queue hands work to whichever lane claims next.
    const served = servedLaneId(result, lane)
    tried.add(served)
    // Remember it so the queue routes the retry to a different egress IP: the
    // same lane would reproduce the same rejection or exhausted bucket.
    queue.avoidLane(entry, served)
    last = result
    entry.outcome = result
    if (result.kind === 'ok') {
      lanes.record(served, 'ok', result.ms || 0)
      return result
    }

    if (result.kind === 'empty') {
      // Upstream returned 200 with no text. Measured on muse-spark: 4 of 5
      // requests came back this way while 1 in 5 was fine, so this is upstream
      // flakiness, not a dead lane. Retry on a different egress IP instead of
      // handing the client an empty answer, but count it separately so a model
      // that is merely flaky is not punished by the hard-failure retry budget.
      empties++
      lanes.record(served, 'empty', result.ms || 0)
      entry.outcome = result
      // Check the cap BEFORE requeueing. Requeueing first let a lane claim and
      // serve the entry during the backoff sleep, after the loop had already
      // decided to stop: five upstream calls for a request allowed four.
      if (empties > RETRY_LIMIT) return result
      if (!entry.committed) {
        queue.requeue(entry, { front: true })
        await sleep(Math.min(2000, 150 * empties))
        continue
      }
      return result
    }

    attempt++

    // Record the outcome BEFORE deciding whether to retry. An earlier version
    // returned here first, so a permanent failure was never counted: the panel
    // showed failed=0 while clients were being refused, and model health never
    // tripped, which is why the misleading message kept coming back.
    if (result.kind === 'limited') {
      // This egress IP is spent. Retire the lane so the orchestrator replaces it
      // with a runner holding a fresh bucket, then hand the work back. Recorded
      // once, as limited: counting it as a failure too inflated failed= and
      // tripped model quarantine for what is a per-IP quota event.
      lanes.record(served, 'limited', result.ms || 0)
      lanes.retire(served, 'egress bucket exhausted')
      // A retired lane is gone from the registry, so this only matters if retire
      // could not find it; keeping it costs nothing and closes the case where the
      // record was pruned first.
      queue.avoidLane(entry, served)
      // `limited` retires a lane on every pass, so it must respect the attempt
      // cap like every other outcome. It previously requeued unconditionally,
      // burning RETRY_LIMIT+2 attempts and retiring that many lanes from a single
      // request -- seven such requests destroyed the whole pool.
      if (!entry.committed && attempt < MAX_ATTEMPTS) {
        queue.requeue(entry, { front: true })
        await sleep(Math.min(2000, 150 * attempt))
        continue
      }
      // Only a stream that already has text on the wire is "partial". Reporting
      // every exhaustion as partial told clients "(partial)" about requests that
      // had produced nothing.
      return entry.committed ? { ...result, kind: 'partial' } : result
    }

    lanes.record(served, result.kind === 'timeout' ? 'timeout' : 'failed', result.ms || 0)
    // Log each attempt's rejection for diagnosis, but do not COUNT it toward model
    // health here: a single bad request makes two attempts, and counting per attempt
    // meant two malformed requests from one client quarantined the model for everyone.
    // Health is updated once per request, in ensureDispatched.
    logAttemptFailure(entry.job.model, result)

    // A rejected fingerprint is not going to change on another IP. Everything
    // else gets at most one more attempt: the two models whose endpoint was
    // permanently gone are no longer advertised, so a 4xx on a listed model is
    // most likely transient and deserves a second try -- but not three, which is
    // what turned one rejection into three full round trips for the client.
    const isRetryable = RETRYABLE.has(result.kind) || (result.kind === 'provider_error' && result.outage)
    if (result.kind === 'gate' || attempt >= MAX_ATTEMPTS || !isRetryable) {
      return last
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
    let result
    try {
      result = await dispatch(entry)
    } catch (e) {
      // A throw here must still settle the client and leave the queue clean:
      // an entry left behind is served later by a lane for nobody, and the lease
      // reaper hands it back in a loop.
      console.error('[relay] dispatch threw:', e?.stack || e)
      result = { kind: 'error', status: 0, ms: 0, data: null, raw: String(e?.message || e) }
    }
    // Model health is counted ONCE per request, from the last real upstream outcome.
    // It used to be counted per attempt, and one request makes up to two attempts, so
    // two malformed requests from a single client reached the quarantine threshold
    // and blacked the model out for every other client. An abandoned request or one
    // that never reached a lane says nothing about the model, so it is not counted.
    if (result.kind !== 'abandoned' && entry.outcome) {
      noteModelResult(entry.job.model, entry.outcome, { log: false })
    }
    // Single exit point: removes the entry from pending and inflight and settles
    // the client. Anything left behind here becomes zombie work that a lane serves
    // for nobody and the lease reaper hands back around the loop.
    queue.finish(entry, result)
  })()
}

export function acceptLaneResult (laneId, entryId, result) {
  lanes.heartbeat(laneId)
  // The entry is looked up in `inflight`, not in a dispatcher-registered map.
  // Delivery used to require a parked dispatcher, but after a requeue the
  // dispatcher sleeps before its next attempt, and a fast lane can claim and
  // answer inside that window -- the result was then dropped and the client hung
  // until the process restarted. The entry object outlives every requeue, so
  // keying on it makes the handoff independent of timing.
  const entry = queue.inflight.get(entryId)
  if (!entry) return false   // unknown, finished, or abandoned: nothing is owed
  // A result from a lane that no longer holds the claim must be dropped. That
  // lane's work was handed back to the queue and may already be served by another
  // lane; accepting this one would settle the client with the wrong answer and
  // leave the live lane streaming into a closed response.
  if (entry.claimedBy && entry.claimedBy !== laneId) return false

  // Record which parts the client already saw, so the streaming path can avoid
  // emitting the same content twice while still sending what it never got.
  entry.streamedText = Boolean(result.streamedText ?? result.streamed)
  entry.streamedTools = Boolean(result.streamedTools)
  entry.streamed = Boolean(result.streamed)
  if (typeof result.streamedTextLen === 'number') {
    entry.streamedTextLen = Math.max(entry.streamedTextLen || 0, result.streamedTextLen)
  }

  // Which lane really served this, for retry attribution and retirement.
  deliverResult(entry, { ...result, laneId })
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
    temperature, top_p: topP, stream, tools, tool_choice: toolChoice, parallel_tool_calls: parallelToolCalls,
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
      toolChoice,
      parallelToolCalls,
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

  // Declared at function scope because the terminal frames below reuse them.
  // Before streaming is set up they are no-ops, so nothing can throw if the
  // request turned out not to be a stream.
  let backlog = []
  let backlogBytes = 0
  let paused = false          // the socket buffer is full: hold further chunks here
  let pendingEnd = false      // end() was requested while data was still queued
  let endTimer = null
  let writeSse = () => {}
  let drainBacklog = () => {}
  // Non-streaming responses are ended directly; the streaming block replaces this
  // with a version that waits for queued data to reach the client first.
  let endWhenDrained = () => {
    try { if (!res.writableEnded && !res.destroyed) res.end() } catch { /* client gone */ }
  }

  if (stream) {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
      'Access-Control-Allow-Origin': '*',
    })

    // Every write to this response goes through writeSse, so the keepalive can
    // never jump ahead of real data that is still queued behind a full socket
    // buffer.
    //
    // Node's res.write() returning false means the chunk WAS accepted and the
    // buffer is now above its high-water mark -- it is a request to stop, not a
    // refusal. The first version queued the chunk again as well, so it was sent
    // twice: measured, 100 deltas of 60KB to a slow reader arrived as 152 frames
    // with 52 duplicates, and duplicated tool-argument fragments are corrupt JSON.
    // A chunk is therefore written exactly once; `paused` only decides whether the
    // NEXT chunk may be written now or must wait in the backlog.
    backlog = []
    backlogBytes = 0

    writeSse = (chunk) => {
      if (res.writableEnded || res.destroyed) return
      if (paused) {
        backlog.push(chunk)
        backlogBytes += chunk.length
        if (backlogBytes > MAX_BACKLOG_BYTES) {
          // The client has stopped reading entirely. Nothing else can help it, and
          // buffering without limit is how the relay runs out of memory.
          console.error(`[relay] client too far behind (${backlogBytes} bytes queued); ` +
            'closing the stream')
          res.destroy()
        }
        return
      }
      if (!res.write(chunk)) paused = true
    }

    const finishEnd = () => {
      clearTimeout(endTimer)
      try { if (!res.writableEnded && !res.destroyed) res.end() } catch { /* client gone */ }
    }

    drainBacklog = () => {
      paused = false
      while (backlog.length && !res.writableEnded && !res.destroyed) {
        // Take the chunk BEFORE writing it: it is accepted whether or not write()
        // reports pressure, so leaving it at the head re-sent it on every drain.
        const chunk = backlog.shift()
        backlogBytes -= chunk.length
        if (!res.write(chunk)) { paused = true; return }
      }
      if (pendingEnd && !paused && !backlog.length) finishEnd()
    }
    res.on('drain', drainBacklog)

    // Ending a response while data is still queued drops that data: the final
    // content, finish_reason and [DONE] were all lost to a slow reader (48 of 60
    // frames, no [DONE], clean EOF). So end only once the backlog has been written,
    // with a bound so a client that never reads cannot hold the socket forever.
    endWhenDrained = () => {
      if (res.writableEnded || res.destroyed) return
      if (!paused && !backlog.length) { finishEnd(); return }
      pendingEnd = true
      endTimer = setTimeout(() => {
        console.error('[relay] client did not drain the tail of the stream in time; closing')
        res.destroy()
      }, END_DRAIN_TIMEOUT_MS)
      endTimer.unref?.()
    }
    res.on('close', () => clearTimeout(endTimer))

    // A comment line: every conformant SSE client ignores it, but it resets the
    // idle timers of any proxy in between. Without it a large prefill (measured
    // 237s TTFT at 850k tokens) is silently dropped by the proxy at its idle
    // timeout, and because the 200 and content-type are already committed the
    // client sees a network error instead of a diagnosable one.
    //
    // It cannot add latency: 15 bytes every 15s, and it *removes* the wasted
    // round trip of a dropped-and-retried request.
    const keepalive = setInterval(() => {
      writeSse(`: keepalive ${Date.now()}\n\n`)
    }, STREAM_KEEPALIVE_MS)
    keepalive.unref?.()
    const stopKeepalive = () => clearInterval(keepalive)
    res.on('close', stopKeepalive)

    writeSse(`data: ${JSON.stringify({
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
      // writeSse checks writability, so a client that disconnected mid-stream does
      // not turn an in-flight delta into a throw.
      writeSse(`data: ${JSON.stringify({
        id: streamId, object: 'chat.completion.chunk', created: streamCreated, model,
        choices: [{ index: 0, delta, finish_reason: null }],
      })}\n\n`)
    })
  }

  // The resolver is installed BEFORE dispatch starts. Dispatch reaches
  // queue.finish() -> entry.resolve() on a fast path (an immediately available
  // lane), and if that ran first the resolve would be missing and the client's
  // promise would never settle -- a request held until the process restarts.
  const settled = new Promise((resolve) => { entry.resolve = resolve })

  // If the client hangs up mid-flight, stop caring immediately. Otherwise the
  // work stays queued and a lane burns a full upstream call on a response nobody
  // will read.
  const onClientGone = () => { if (!entry.done) queue.abandon(entry) }
  res.on('close', onClientGone)

  ensureDispatched(entry)

  const result = await settled
  res.off?.('close', onClientGone)
  streamers.delete(entry.id)

  // The client disconnected (or the entry was dropped) while the lane worked.
  // Nothing can be written to it, so finish silently rather than throwing
  // ERR_STREAM_WRITE_AFTER_END into the request handler.
  if (result.kind === 'abandoned' || entry.abandoned || res.writableEnded || res.destroyed) {
    try { if (!res.writableEnded) res.end() } catch { /* client already gone */ }
    return
  }

  if (result.kind !== 'ok') {
    // Upstream is unusable for this request. That is NOT a client rate limit, so
    // it must not be reported as 429.
    if (stream) {
      // Headers are long gone, so the failure is reported in-band.
      const err = {
        error: {
          // The retry ceiling is 2 attempts, not RETRY_LIMIT: a failure that
          // another egress IP cannot fix is not worth three round trips. Reporting
          // "retried 3x" when only one retry happened sent people looking for
          // retries that were never made.
          // Report how many times the request was actually handed to a lane, not the
          // generic cap. `empty` outcomes are retried on their own counter, so
          // quoting MAX_ATTEMPTS-1 was frequently wrong -- it said "retried 1x"
          // on requests that had actually been tried several times, which made a
          // token-budget problem look like a single unlucky attempt.
          message: `no lane could serve this request (${result.kind}); ` +
            `tried ${entry.attempts || 1} time(s) across ${result.kind === 'empty' ? 'fresh lanes' : 'lanes'}`,
          type: 'upstream_unavailable',
        },
      }
      if (!res.writableEnded && !res.destroyed) {
        try {
          // Through writeSse so the terminal frames cannot jump ahead of data still
          // queued behind a full socket buffer.
          writeSse(`data: ${JSON.stringify(err)}\n\n`)
          writeSse('data: [DONE]\n\n')
          // End only after the backlog has been written, or a slow but live client
          // loses the tail of the answer.
          endWhenDrained()
        } catch { /* client already gone */ }
      }
      return
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
  // Every terminal write goes through one guard: the client may have hung up
  // while the lane was still working, and writing to a finished response throws.
  // writeSse additionally preserves ordering against anything still backlogged.
  const safeWrite = (frame) => {
    if (res.writableEnded || res.destroyed) return false
    try { writeSse(frame); return true } catch { return false }
  }
  const chunk = (delta, finish = null) => safeWrite(`data: ${JSON.stringify({
    id, object: 'chat.completion.chunk', created, model,
    choices: [{ index: 0, delta, finish_reason: finish }],
  })}\n\n`)

  // Text that already arrived through /lane/delta must not be replayed, or the
  // client sees it twice. The comparison is by length, not by a flag: a lost batch
  // leaves the streamed text shorter than the folded answer, and the missing tail
  // is exactly what the client needs to finish the sentence it was shown.
  const streamedTextLen = entry.streamedTextLen || 0
  const fullText = completion.choices[0]?.message?.content ?? ''
  if (streamedTextLen < fullText.length) {
    // Send only the part the client has not seen. When nothing was streamed this
    // is the whole answer; when a batch was lost it is the missing suffix.
    chunk({ content: fullText.slice(streamedTextLen) })
  }

  const buffered = completion.choices[0]?.message
  const bufferedCalls = Array.isArray(buffered?.tool_calls) ? buffered.tool_calls : []
  // Reconcile each call against what the client actually received. Fragments are
  // concatenated by a harness, so a call whose streamed arguments stopped short is
  // completed by sending ONLY the missing suffix -- appending it yields exactly the
  // folded arguments. This is what repairs a batch lost in flight: before, the
  // client was left holding a truncated JSON string and reported the call as
  // missing required properties ("file_path" / "old_string" / "new_string").
  if (bufferedCalls.length) {
    const sent = entry.streamedToolArgs || new Map()
    const sentNames = entry.streamedToolNames || new Map()
    const repair = []
    for (let i = 0; i < bufferedCalls.length; i++) {
      const c = bufferedCalls[i]
      const fullName = c.function?.name || ''
      const gotName = sentNames.get(i) || ''
      const fullArgs = typeof c.function?.arguments === 'string' ? c.function.arguments : ''
      const got = sent.get(i) || ''
      if (!entry.streamedTools) {
        // Nothing was streamed: send the whole call, name and id included.
        repair.push({ ...c, index: i })
        continue
      }
      // If the client's received name does not match the full folded name, the name
      // was split across batches or truncated in flight. Restate the call in full.
      if (fullName !== gotName) {
        repair.push({ index: i, ...c })
        continue
      }
      // Identity is never repeated: a client appending name fragments would turn
      // "edit" into "editedit". Only the argument tail, when one is missing.
      if (fullArgs.length > got.length && fullArgs.startsWith(got)) {
        repair.push({ index: i, function: { arguments: fullArgs.slice(got.length) } })
      } else if (fullArgs !== got) {
        // The client's copy is not a prefix of the folded call, so appending
        // cannot repair it. Restate the call in full rather than leave a harness
        // with JSON it cannot parse.
        repair.push({ index: i, ...c })
      }
    }
    if (repair.length) chunk({ tool_calls: repair })
  }
  // A tool call must surface as finish_reason "tool_calls" so the harness stops
  // reading text and executes the call. Defaulting to "stop" made a harness treat
  // the turn as a finished answer -- the model said what it was about to do and
  // the stream ended, which is precisely the "fake promise" symptom.
  const finishReason = bufferedCalls.length
    ? 'tool_calls'
    : (completion.choices[0]?.finish_reason || 'stop')
  chunk({}, finishReason)
  if (completion.usage) {
    safeWrite(`data: ${JSON.stringify({
      id, object: 'chat.completion.chunk', created, model, choices: [], usage: completion.usage,
    })}\n\n`)
  }
  // [DONE] is emitted on every completed stream, including the ones the client has
  // already walked away from: a harness that half-read the stream depends on it to
  // close its own parser rather than treating the connection drop as a crash.
  safeWrite('data: [DONE]\n\n')
  endWhenDrained()
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
  if (lane.retired) {
    // This egress IP's bucket is spent. Admitting it again would send work to a
    // lane that can only answer 429, which then retires it in a loop.
    return json(res, 410, { error: 'lane retired', laneId: body.laneId, retiredAt: lane.retiredAt })
  }
  // A lane running different code than this relay was built with is the failure
  // that hid tool-call corruption for days. Say so in the log the moment it joins.
  if (!lane.codeWarned && lane.codeVersion !== CODE_VERSION) {
    lane.codeWarned = true
    console.error(`[relay] STALE LANE CODE: ${lane.id} runs ` +
      `${lane.codeVersion || 'an unversioned build (predates version reporting)'}, ` +
      `relay expects ${CODE_VERSION}. The runners are executing different lane code than ` +
      'this jar was built from -- push src/ to the runner repo and restart the runs.')
  }
  return json(res, 200, { ok: true, laneId: lane.id, queueDepth: queue.pending.length, codeVersion: CODE_VERSION })
}

async function laneHeartbeat (req, res) {
  if (!authorized(req)) return json(res, 403, { error: 'bad token' })
  const body = await readBody(req).catch(() => ({}))
  const lane = lanes.heartbeat(body.laneId)
  if (!lane) return json(res, 404, { error: 'unknown lane' })
  // A lane heartbeats while it works, so everything it holds is provably live
  // and must not be handed to another lane as a stale claim.
  queue.touchLane(body.laneId)
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

  const waitMs = Number(url.searchParams.get('wait') || 20000)
  const safeWait = Number.isFinite(waitMs) && waitMs > 0 ? waitMs : 20000

  // A lane on stale code is not given work while a current one is available.
  //
  // Lanes PULL work, so preferring current lanes in pickLane only chose a hint: any
  // lane polling /lane/claim took the entry regardless. Measured: with three current
  // lanes and one old one, the old one served 2 of 12 requests and exactly those 2
  // came back with a doubled tool name ("bashbash") and unparseable arguments. A
  // corrupted tool call is worse than a short wait for a current lane, so the stale
  // lane just idles its poll. If NO current lane is live the stale ones are used
  // anyway -- a degraded service beats none -- and the STALE LANE CODE warning has
  // already said why. ALLOW_STALE_LANES=1 turns this off.
  const me = lanes.lanes.get(laneId)
  if (me && me.codeVersion !== CODE_VERSION && config.ALLOW_STALE_LANES !== '1' &&
      lanes.liveLanes(LANE_STALE_MS).some((l) => l.codeVersion === CODE_VERSION && l.status !== 'exhausted')) {
    await sleep(Math.min(safeWait, 5000))
    lanes.heartbeat(laneId)
    return json(res, 204, {})
  }

  const entry = await queue.take(laneId, safeWait)
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

  // The entry is resolved through the queue, the same way a result is: `inflight`
  // is the one place an entry is guaranteed to be while a lane is serving it.
  const entry = queue.inflight.get(entryId)
  // No in-flight entry means NO lane holds a claim right now (it was requeued after a
  // reap, or finished). The claim check below sat inside `if (entry)`, so it was
  // skipped in exactly that case and a lane that had lost its claim could still write
  // into the client's stream: observed, "GHOST FROM LANE THAT LOST CLAIM" delivered.
  if (!entry) {
    return json(res, 409, { ok: false, error: 'entry is not claimed by any lane' })
  }
  if (entry) {
    // Same claim guard as /lane/result. Without it, a lane that lost its claim
    // (lease reaped after a heartbeat gap, relay stall, GC pause) keeps streaming
    // into a client that another lane is now also streaming into. Its final result
    // is correctly rejected, so the client silently ends up with interleaved text
    // from two different answers.
    if (entry.claimedBy && entry.claimedBy !== body.laneId) {
      return json(res, 409, { ok: false, error: 'lane does not hold this claim' })
    }
    entry.committed = true
    // Exactly how much of each stream was delivered, not just whether any was.
    // A batch can be lost in flight (relay restart, proxy reset), and the boolean
    // form then claimed the client was complete when its tool arguments were a
    // truncated JSON string -- the "missing required property" failure. Recording
    // the actual bytes lets the finalizer send only what is missing.
    if (typeof body.text === 'string' && body.text) {
      entry.streamedText = true
      entry.streamedTextLen = (entry.streamedTextLen || 0) + body.text.length
    }
    if (Array.isArray(body.toolCalls) && body.toolCalls.length) {
      entry.streamedTools = true
      const args = entry.streamedToolArgs || (entry.streamedToolArgs = new Map())
      const names = entry.streamedToolNames || (entry.streamedToolNames = new Map())
      for (const c of body.toolCalls) {
        const i = typeof c.index === 'number' ? c.index : 0
        if (typeof c.function?.arguments === 'string') {
          const prev = args.get(i) || ''
          args.set(i, prev + c.function.arguments)
        }
        if (typeof c.function?.name === 'string' && c.function.name) {
          const prevN = names.get(i) || ''
          names.set(i, prevN ? prevN + c.function.name : c.function.name)
        }
      }
    }
  }
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
          build: `${config.BUILD_ID} (${config.BUILD_BUILT})`,
          laneCode: laneCodeStats(),
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

      // A lane's claim is only stale if the LANE is stale. Claim age alone is not
      // enough: a large prefill or a long tool turn legitimately holds a claim for
      // minutes while the lane heartbeats the whole time.
      if (path.startsWith('/lane/')) {
        queue.reapStaleClaims((laneId) => {
          const rec = laneId ? lanes.lanes.get(laneId) : null
          return Boolean(rec && Date.now() - rec.lastSeen < LANE_STALE_MS)
        })
      }
      if (method === 'POST' && path === '/lane/register') return laneRegister(req, res)
      if (method === 'POST' && path === '/lane/heartbeat') return laneHeartbeat(req, res)
      if (method === 'GET' && path === '/lane/claim') return laneClaim(req, res)
      if (method === 'POST' && path === '/lane/result') return laneResult(req, res)
      if (method === 'POST' && path === '/lane/delta') return laneDelta(req, res)

      if (method === 'GET' && (path === '/v1/models' || path === '/models')) {
        return json(res, 200, listModelsPayload(`${config.BUILD_ID} (${config.BUILD_BUILT})`))
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
    console.log(`[relay] build ${config.BUILD_ID} built ${config.BUILD_BUILT}`)
    console.log(`[relay] listening on http://${HOST}:${PORT}`)
    console.log(`[relay] client API: /v1/models, /v1/chat/completions  (models: ${MODELS.length})`)
    console.log(`[relay] dashboard:  /dashboard`)
    console.log(`[relay] max lanes ${MAX_LANES}, retry limit ${RETRY_LIMIT}, max hold ${MAX_HOLD_MS}ms`)
  })
  return server
}

if (process.argv[1] && process.argv[1].endsWith('server.js')) start()