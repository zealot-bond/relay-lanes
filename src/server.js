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
import { splitModelId, PROVIDER } from './models.js'
import * as catalogue from './catalogue.js'
import { config } from './config.js'
import { CODE_VERSION } from './codeversion.js'
import { directEnabled, directStats, serveDirect } from './direct.js'
import { zenBase } from './fingerprint.js'
import * as exhaustion from './exhaustion.js'
import {
  escalate, stats as escalateStats, onEscalate,
  directAvailable, directBlocked, noteDirectLimited, noteDirectOk, noteDirectProbed,
} from './escalate.js'

// Config is read from config.js, which resolves baked credentials, then
// start.properties, then the environment -- so a baked PORT is honoured even
// though main.js is the process entry point.
const PORT = config.PORT
const HOST = config.HOST
const RELAY_TOKEN = config.RELAY_TOKEN
const MAX_LANES = config.MAX_LANES
const MAX_HOLD_MS = config.MAX_HOLD_MS
/** Use this host as a BACKUP when no lane can serve, even with lanes as the primary path. */
const directFallbackEnabled = () => String(config.DIRECT_FALLBACK) === '1'
// Lanes that could actually serve: fresh, and not retired for an exhausted bucket. A pool
// of retired lanes is the case the direct backup exists for -- otherwise "lanes exist" would
// look like service while every one of them answers 429.
const usableLanes = () => lanes.liveLanes(LANE_STALE_MS).filter((l) => l.status !== 'exhausted')
const RETRY_LIMIT = config.RETRY_LIMIT
const STREAM_KEEPALIVE_MS = config.STREAM_KEEPALIVE_MS
// Parsed heartbeat for clients whose idle watchdog counts SSE EVENTS, not bytes.
const STREAM_THINKING_HEARTBEAT_MS = config.STREAM_THINKING_HEARTBEAT_MS
const MAX_BACKLOG_BYTES = config.MAX_BACKLOG_BYTES
// How long a finished stream may wait for a slow client to read its tail.
const END_DRAIN_TIMEOUT_MS = config.END_DRAIN_TIMEOUT_MS
const MAX_REQUEST_BYTES = config.MAX_REQUEST_BYTES
const BODY_DRAIN_TIMEOUT_MS = config.BODY_DRAIN_TIMEOUT_MS

export const queue = new WorkQueue({ claimLeaseMs: 120000, avoidTtlMs: 60000 })
export const lanes = new LaneRegistry({ maxLanes: MAX_LANES })

const json = (res, status, obj, headers = {}) => {
  const body = JSON.stringify(obj)
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body), ...headers })
  res.end(body)
}

const BODY_TOO_LARGE = 'BODY_TOO_LARGE'

/** A request body the relay refuses, carrying the status the client should see. */
function bodyTooLarge (size, limit) {
  const err = new Error(
    `request body too large: ${size} bytes exceeds the ${limit}-byte limit. ` +
    'Compact the session (or shorten the conversation) and retry.'
  )
  err.code = BODY_TOO_LARGE
  err.statusCode = 413
  return err
}

/**
 * Read a JSON request body without ever destroying the socket mid-upload.
 *
 * This used to reject on overflow and immediately req.destroy(). The relay sits
 * behind cloudflared, and an origin that closes before sending a response becomes a
 * bodiless 502 at the client -- not a 413 and not a connection error. A long agent
 * session that crossed the old 16 MB cap therefore failed every retry with
 * `502 status code (no body)` while smaller sessions worked (98 in 22 minutes,
 * measured 2026-10-28).
 *
 * So an oversized body is DRAINED (the data handler keeps the stream flowing but
 * stops buffering) and rejected only once the request has ended. The handler can
 * then write a real 413 that survives the trip back through the tunnel. A client
 * that keeps uploading forever is bounded by BODY_DRAIN_TIMEOUT_MS.
 */
function readBody (req, limitBytes = MAX_REQUEST_BYTES) {
  return new Promise((resolve, reject) => {
    const declared = Number(req.headers['content-length'])
    let size = 0
    let overflow = Number.isFinite(declared) && declared > limitBytes ? declared : 0
    const chunks = []
    let timer = null

    const armDrainTimer = () => {
      if (timer) return
      timer = setTimeout(() => {
        try { req.destroy() } catch { /* already gone */ }
        reject(bodyTooLarge(size || overflow, limitBytes))
      }, BODY_DRAIN_TIMEOUT_MS)
      timer.unref?.()
    }

    req.on('data', (c) => {
      size += c.length
      if (!overflow && size > limitBytes) overflow = size
      if (overflow) {
        // Stop holding the bytes, but keep consuming them so the response is not
        // lost to a TCP reset.
        chunks.length = 0
        armDrainTimer()
        return
      }
      chunks.push(c)
    })
    req.on('end', () => {
      if (timer) clearTimeout(timer)
      if (overflow) return reject(bodyTooLarge(overflow, limitBytes))
      const raw = Buffer.concat(chunks).toString()
      if (!raw) return resolve({})
      try { resolve(JSON.parse(raw)) } catch { reject(new Error('invalid JSON')) }
    })
    req.on('error', (e) => {
      if (timer) clearTimeout(timer)
      reject(e)
    })

    if (overflow) armDrainTimer()
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

const UNCOUNTED_KINDS = new Set(['ok', 'limited', 'ip_blocked', 'empty', 'transport', 'timeout', 'error'])

// Rate limiting is the single most important thing to see when the relay goes quiet, and
// `limited` is in UNCOUNTED_KINDS because it must not count toward MODEL health (one IP's
// exhausted bucket says nothing about the model). Being uncounted also meant it was never
// logged, so /health reported `recentFailures: []` through an entire rate-limit storm --
// the operator saw served=0 and an empty log and had nothing to go on. Tracked separately,
// and still not counted.
const rateLimits = { count: 0, lastAt: null, lastModel: null, lastDetail: '' }

/**
 * What the CLIENT experienced, which is not the same question as what the lanes did.
 *
 * `failed`/`timeouts` are lane outcomes. A client can see "terminated" or a read
 * timeout while every lane accepted its work and answered it: the eyeball's own
 * connection through the edge died (lossy Wi-Fi, a mobile hotspot, a carrier NAT),
 * and the relay only ever saw its socket close. Those requests were counted nowhere,
 * so /health reported a perfect relay through an incident the user could see. These
 * counters make the difference visible instead of arguing about it.
 */
const clientStats = {
  streams: 0,        // streaming responses begun
  gone: 0,           // client hung up before its answer finished
  partial: 0,        // upstream failed after text had already reached the client
  lastGoneAt: null,
  lastPartialAt: null,
  lastPartialKind: null,
}
const processStartedAt = Date.now()

function noteClientGone () {
  clientStats.gone++
  clientStats.lastGoneAt = new Date().toISOString().slice(11, 19)
}

function notePartial (kind) {
  clientStats.partial++
  clientStats.lastPartialAt = new Date().toISOString().slice(11, 19)
  clientStats.lastPartialKind = kind || 'unknown'
}

function noteRateLimit (model, result) {
  rateLimits.count++
  rateLimits.lastAt = new Date().toISOString().slice(11, 19)
  rateLimits.lastModel = model
  rateLimits.lastDetail = String(result.raw || '').replace(/\s+/g, ' ').trim().slice(0, 160)
  noteFailure(model, result)
}
// `gone` is counted once per request like provider_error, so it is not logged per attempt.

/** Record an attempt's rejection in recentFailures without touching the health counter. */
function logAttemptFailure (model, result) {
  if (!UNCOUNTED_KINDS.has(result.kind)) noteFailure(model, result)
}

/** Was this egress IP BLACKLISTED, rather than merely out of quota?
 *
 * The provider says: "OpenCode's free models aren't available from your current network
 * because we've detected abusive traffic from this IP address."
 *
 * That is a per-IP reputation block: the remedy is a different egress IP, not a wait.
 * It arrives as a 403 carrying FreeTierError, the same envelope the fingerprint gate
 * uses -- so the two are told apart by their words, not by their status.
 */
function ipIsBlacklisted (result) {
  return /abusive traffic from this IP|aren't available from your current network/i.test(String((result && result.raw) || ''))
}

/** One phrase for why an egress was refused, used by both error paths. */
function limitHeadline (result) {
  return ipIsBlacklisted(result)
    ? 'this egress IP is blacklisted by the provider for abusive traffic'
    : 'free-tier rate limit reached'
}

/** Did the upstream actually say it was a rate limit, whatever status carried it? */
function isRateLimitBody (result) {
  return /FreeUsageLimitError|rate[_ ]limit exceeded|rate[_ ]limit reached|abusive traffic from this IP|aren't available from your current network/i.test(String((result && result.raw) || ''))
}

/**
 * The gate's 429 is not a quota.
 *
 * Measured 2026-10-26 by bisection: the free tier returns
 * `429 FreeUsageLimitError ... Free models work in OpenCode` -- with a midnight-UTC
 * Retry-After -- to any request it does not recognise as the first-party client, and
 * 200 to the same body sent by Bun with an `<env>` block and the four tool names. It is
 * dressed exactly like exhaustion, which is why it was misread as a spent bucket for two
 * days while a real client on the same IP kept working.
 *
 * When one reaches a client, the relay's own request shape is wrong. Telling the operator
 * to wait for the reset would send them to the wrong problem, so say what it is.
 */
function isGateBody (result) {
  return /Free models work in OpenCode/i.test(String((result && result.raw) || ''))
}

/** Advice that matches the actual cause, shared by the streaming and JSON error paths. */
function limitAdvice (result) {
  if (isGateBody(result)) {
    return 'This is the free tier\'s fingerprint gate, not a quota: the provider did not ' +
      'recognise the request as the first-party client. Check that the Bun egress proxy is ' +
      'running (EGRESS_ENABLED=1 and a working BUN_BIN) and that the request carries an ' +
      '<env> block with a working directory and the four gateway tool names. Retrying will ' +
      'not help until that is fixed.'
  }
  return directEnabled()
    ? 'DIRECT_MODE sends every model through this one egress IP, so the whole catalogue ' +
      'goes quiet together and retrying cannot help. Set DIRECT_MODE=0 to rotate across ' +
      'runner IPs, or reduce request volume.'
    : 'This lane\'s egress IP is exhausted and the relay retires it, so a client should ' +
      'not normally see this; if it repeats, every egress IP is being refused.'
}

/** A gate 429 carries a fake Retry-After, so naming a reset time would be a lie. */
function limitResetNote (result, retryAfter) {
  if (isGateBody(result) || !(retryAfter >= 600)) return ''
  return ` The window reopens at ${new Date(Date.now() + retryAfter * 1000).toISOString().slice(11, 16)} UTC.`
}

/**
 * Seconds to suggest before retrying.
 *
 * Honoured from the upstream when it tells us (Retry-After, x-ratelimit-reset), because
 * guessing short makes clients burn more of a quota that is already exhausted. Otherwise
 * the free tier is hourly-ish, so say so rather than inviting an immediate retry.
 */
function rateLimitRetryAfter (result) {
  // The upstream's own header wins: it is the only value that reflects the real window.
  const explicit = Number(result && result.retryAfter)
  if (Number.isFinite(explicit) && explicit > 0) return Math.ceil(explicit)
  const raw = result && result.raw
  const m = /retry[-_ ]?after["':\s]*(\d+)/i.exec(String(raw || ''))
  if (m) return Math.max(1, Number(m[1]))
  return Number(process.env.RATE_LIMIT_RETRY_AFTER_S || 900)
}

/**
 * The provider's own words about a rejection, as one short line.
 *
 * Falls back to the quarantined-model note when the attempt carried no body, so a
 * refusal is never reported without a reason.
 */
function upstreamDetail (result, model) {
  const body = String((result && result.raw) || '').slice(0, 200).replace(/\s+/g, ' ').trim()
  return body || ((model && (modelHealthOf(model) || {}).note) || '')
}

// A withdrawn model is not coming back within a two-minute window, so it is held
// off much longer. Retrying it every couple of minutes cost six upstream calls per
// request from every client for as long as it stayed advertised.
const GONE_QUARANTINE_MS = 60 * 60 * 1000

// A prompt longer than the model's window. The wording is kept because the harness recognises
// it and compacts the conversation before retrying; the model itself is healthy.
export function contextMessage (result, model) {
  const detail = upstreamDetail(result, model)
  return `${PROVIDER}/${model}: this request is longer than the model's maximum context length. ` +
    'Compact the conversation and send it again.' + (detail ? ` Upstream: ${detail}` : '')
}

export function noteModelResult (model, result, { log = true } = {}) {
  if (result.kind === 'context') return
  const h = modelHealth.get(model) || { fails: 0, lastKind: null, note: '', until: 0 }
  if (result.kind === 'gone') {
    // Count it up to the threshold so the refusal kicks in immediately, rather than
    // making the next two requests each pay for the same lesson.
    h.fails = Math.max(h.fails, HEALTH_THRESHOLD)
    h.lastKind = 'gone'
    h.note = 'the provider withdrew this model (HTTP 410/404); it is not coming back'
    h.until = Date.now() + GONE_QUARANTINE_MS
    modelHealth.set(model, h)
    if (log) noteFailure(model, result)
    return
  }
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
  if (result.kind === 'limited' || result.kind === 'ip_blocked' || result.kind === 'empty' ||
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
//   gone           no  - the provider withdrew the model; identical from every IP
const RETRYABLE = new Set(['limited', 'ip_blocked', 'empty', 'transport', 'timeout', 'error'])
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
    const activeLaneId = () => {
      const held = queue.inflight.get(entry.id)
      return held?.claimedBy || lane.id
    }

    const laneGone = () => {
      const held = queue.inflight.get(entry.id)
      if (held && held.claimedAt && Date.now() - held.claimedAt < CLAIM_GRACE_MS) return false
      return stale(activeLaneId())
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
        fin({ kind: 'requeued', status: 0, ms: Date.now() - entry.enqueuedAt, data: null, laneId: activeLaneId() })
      }
    }, 2000)
    poll.unref?.()

    const arm = () => {
      timer = setTimeout(() => {
        waited += ATTEMPT_HARD_MS
        const currentLane = activeLaneId()
        const rec = currentLane ? lanes.lanes.get(currentLane) : null
        const alive = rec && Date.now() - rec.lastSeen < LANE_STALE_MS
        if (alive && waited < ATTEMPT_MAX_MS) return arm()
        fin({ kind: 'timeout', status: 0, ms: waited, data: null, laneId: currentLane })
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

    // The upstream told us the window reopens hours from now. Every egress IP sees the
    // same window, so neither another lane nor the direct path can answer: do not call
    // either. The client gets the provider's own reset time immediately instead of
    // waiting out a lane round trip to be told the same thing.
    if (exhaustion.active()) {
      const spent = exhaustion.result(entry.job.model)
      entry.outcome = spent
      return entry.committed ? { ...spent, kind: 'partial' } : spent
    }

    // DIRECT_MODE: call the upstream here, in-process, and skip the lane pool entirely.
    // Placed before lane selection so nothing touches the queue, a claim, or a heartbeat.
    // DIRECT: either primary (DIRECT_MODE=1) or the backup used when no lane can serve
    // (DIRECT_FALLBACK, the default). Lanes are primary again as of 2026-10-17.
    //
    // Skipped entirely while the direct bucket is known spent -- asking anyway costs a
    // wasted upstream call and up to ~1.5s of latency, to learn something already known.
    // After DIRECT_RECOVER_MS one probe is allowed through, because the quota resets and
    // this IP becomes free again.
    const directOk = directAvailable() || !noteDirectProbed()
    const tryDirect = directOk && !entry.escalated &&
      (directEnabled() || (!usableLanes().length && directFallbackEnabled()))
    if (tryDirect) {
      attempt++
      const stream = entry.job.stream
      const push = streamers.get(entry.id)
      let result
      try {
        result = await serveDirect(entry.job, {
          onDelta: stream && push ? (t) => { entry.streamedText = true; entry.streamedTextLen = (entry.streamedTextLen || 0) + t.length; push(t, null) } : undefined,
          // Thinking is written straight through and deliberately NOT recorded as
          // delivered text: it is not the answer, and marking it as such would make
          // the finaliser believe the client already had content it never received.
          onReasoning: stream && push ? (t) => { push('', null, t) } : undefined,
          onToolCall: () => { if (stream && push) entry.committed = true },
          onToolDelta: stream && push ? (d) => {
            entry.streamedTools = true
            const args = entry.streamedToolArgs || (entry.streamedToolArgs = new Map())
            const names = entry.streamedToolNames || (entry.streamedToolNames = new Map())
            const ids = entry.streamedToolIds || (entry.streamedToolIds = new Map())
            for (const c of d) {
              const i = typeof c.index === 'number' ? c.index : 0
              if (typeof c.id === 'string' && c.id) ids.set(c.id, i)
              if (typeof c.function?.arguments === 'string') args.set(i, (args.get(i) || '') + c.function.arguments)
              if (typeof c.function?.name === 'string' && c.function.name) {
                const prev = names.get(i) || ''
                names.set(i, prev ? prev + c.function.name : c.function.name)
              }
            }
            push('', d)
          } : undefined,
        })
      } catch (e) {
        result = { kind: 'error', status: 0, ms: 0, data: null, raw: String(e?.message || e) }
      }
      entry.outcome = result
      console.log(`[relay] direct ${entry.job.model} -> ${result.kind} (${result.ms}ms) via ${zenBase()}`)
      if (result.kind === 'ok') {
        lanes.record('direct', 'ok', result.ms || 0)
        // The bucket is alive again; the breaker must not keep skipping direct.
        noteDirectOk()
        return result
      }
      if (result.kind !== 'limited') {
        // An outage, a withdrawn model, a bad request: another IP would answer exactly
        // the same. Report it.
        return result
      }

      // This IP's bucket is spent. Escalate rather than fail: arm the runner pool and,
      // once a lane exists, fall through to the normal lane path below. A lane has a
      // DIFFERENT egress IP and therefore a different bucket, so this genuinely answers.
      noteRateLimit(entry.job.model, result)
      // Switch the direct path off, so the NEXT request goes straight to a lane.
      noteDirectLimited()
      // A reset hours away is the same on every egress IP, so escalating to runners
      // cannot answer this and would spend CI minutes proving it. Answer with the reset.
      if (exhaustion.isGlobal(result)) {
        exhaustion.note(result.retryAfter, entry.job.model)
        return result
      }
      const laneReady = () => lanes.liveLanes(LANE_STALE_MS)
        .some((l) => l.status !== 'exhausted')
      if (!(await escalate(laneReady))) {
        // No lane in time. The honest answer is still the 429, with the escalation
        // already under way for the next request.
        return result
      }
      // Mark the entry so the next pass goes to the LANE path.
      //
      // Without this the loop re-entered the direct branch forever: direct -> limited ->
      // escalate -> continue -> direct -> limited, spinning at ~1ms per pass and never
      // reaching the lanes it was waiting for. Caught by test/escalation.mjs.
      entry.escalated = true
      // The direct call told us about the BUCKET, not about this request, so it must not
      // consume an attempt; the lane attempt that follows is the one that counts.
      attempt--
      continue
    }

    const lane = pickLane(tried, busyLanes)
    if (!lane) {
      // Direct is out and no lane has appeared. The pool may be starting, so a short hold
      // is right -- but the client's own deadline must not be spent here. Once we have
      // already seen the rate limit, there is nothing to wait for that this request can
      // usefully do, so answer with the real reason instead of holding for MAX_HOLD_MS
      // (10 minutes) and then reporting it anyway.
      if (directBlocked() && last.kind === 'limited') return last
      // No lane available yet. Hold the client rather than failing them.
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
    if (result.kind === 'limited' || result.kind === 'ip_blocked') {
      // This egress IP is spent. Retire the lane so the orchestrator replaces it
      // with a runner holding a fresh bucket, then hand the work back. Recorded
      // once, as limited: counting it as a failure too inflated failed= and
      // tripped model quarantine for what is a per-IP quota event.
      lanes.record(served, 'limited', result.ms || 0)
      // Make it visible: the count, which model, and the provider's own words.
      noteRateLimit(entry.job.model, result)
      // The window reopens hours from now: this lane's egress was not the variable, so
      // keep it. Retiring it would spend a runner to replace an IP that would report the
      // same reset, and a retry across lanes would report it again.
      if (exhaustion.isGlobal(result)) {
        exhaustion.note(result.retryAfter, entry.job.model)
        return entry.committed ? { ...result, kind: 'partial' } : result
      }
      lanes.retire(served, result.kind === 'ip_blocked'
        ? 'egress IP blacklisted for abusive traffic'
        : 'egress bucket exhausted')
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
    return json(res, e.statusCode || 400, {
      error: {
        message: String(e.message),
        type: e.code === BODY_TOO_LARGE ? 'request_too_large' : 'invalid_request_error',
      },
    })
  }

  const {
    model: rawModel, messages, system, max_tokens: maxTokens, max_completion_tokens: maxCompletion,
    temperature, top_p: topP, stream, tools, tool_choice: toolChoice, parallel_tool_calls: parallelToolCalls,
    reasoning_effort: clientEffort,
  } = payload || {}

  const resolved = catalogue.resolve(rawModel || '')
  const model = resolved?.id
  if (!resolved) {
    return json(res, 404, {
      error: {
        message: `unknown model: ${rawModel}. GET /v1/models lists the models this relay serves.`,
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
      // A variant id (github/x:high) wins, then the client's own reasoning_effort if this model
      // accepts it, then the model's measured default. A client effort the model rejects falls back
      // to the default rather than failing, so a harness that always sends one value still works.
      reasoningEffort: resolved.effort ||
        (typeof clientEffort === 'string' && resolved.entry.efforts.includes(clientEffort) ? clientEffort : undefined) ||
        resolved.entry.defaultEffort || undefined,
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
  // Stops comments and thinking heartbeats as soon as the result is in: a data
  // frame after finish_reason would be invalid ordering for a strict client.
  let stopHeartbeat = () => {}
  // Tracks the last frame a CLIENT PARSER can see. A comment frame is invisible to
  // an SSE event iterator, so a client watchdog (DSH/pi-ai: 300s) does not count it.
  let lastParsedAt = 0
  // Non-streaming responses are ended directly; the streaming block replaces this
  // with a version that waits for queued data to reach the client first.
  let endWhenDrained = () => {
    try { if (!res.writableEnded && !res.destroyed) res.end() } catch { /* client gone */ }
  }

  if (stream) {
    clientStats.streams++
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
      // A comment keeps a proxy's idle timer alive but is dropped by an SSE parser,
      // so a harness that arms its watchdog around the parsed event iterator still
      // aborts a slow prefill. Measured 2026-10-31: DSH's pi-ai watchdog rejected a
      // 200 with "pi-ai stream idle timeout after 300000ms" while the relay had been
      // sending comments every 15s. Whenever the stream has been silent for the
      // thinking-heartbeat interval, also send one space of reasoning: pi-ai yields
      // thinking_start/thinking_delta for a non-empty field, which resets the
      // watchdog, and a space is invisible in the UI. It is never content, never
      // counted as streamed text, and never enters the fold. This also covers a
      // retry after a lane died mid-thinking: the client was already committed to a
      // thinking block, but its watchdog does not know the relay switched lanes.
      if (STREAM_THINKING_HEARTBEAT_MS > 0 &&
          String(config.REASONING_STREAM) !== '0' &&
          Date.now() - lastParsedAt >= STREAM_THINKING_HEARTBEAT_MS) {
        writeSse(`data: ${JSON.stringify({
          id: streamId, object: 'chat.completion.chunk', created: streamCreated, model,
          choices: [{ index: 0, delta: { reasoning_content: ' ' }, finish_reason: null }],
        })}\n\n`)
        lastParsedAt = Date.now()
      }
    }, STREAM_KEEPALIVE_MS)
    keepalive.unref?.()
    stopHeartbeat = () => clearInterval(keepalive)
    res.on('close', stopHeartbeat)

    writeSse(`data: ${JSON.stringify({
      id: streamId, object: 'chat.completion.chunk', created: streamCreated, model,
      choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }],
    })}\n\n`)
    lastParsedAt = Date.now()
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
    streamers.set(entry.id, (text, toolCalls, reasoning) => {
      // Text and tool calls are separate delta shapes. An agent harness reads
      // delta.tool_calls and delta.tool_calls[].function to decide what to run, so
      // these must be emitted in the standard chat format, not folded into text.
      const delta = {}
      // Thinking precedes the answer, so it is emitted first and on its own field.
      // A client that renders `reasoning_content` shows progress during a long
      // think instead of a blank screen; one that ignores it is unaffected.
      // REASONING_STREAM=0 turns the forwarding off without a rebuild.
      if (reasoning && String(config.REASONING_STREAM) !== '0') delta.reasoning_content = reasoning
      if (text) delta.content = text
      if (toolCalls && toolCalls.length) delta.tool_calls = toolCalls
      if (!Object.keys(delta).length) return
      lastParsedAt = Date.now()
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
  const onClientGone = () => { if (!entry.done) { noteClientGone(); queue.abandon(entry) } }
  res.on('close', onClientGone)

  ensureDispatched(entry)

  const result = await settled
  stopHeartbeat()
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
    // Upstream is unusable for this request. Most of these are NOT a client rate limit
    // and must not be reported as 429 -- a 4xx is the endpoint refusing the request and
    // identical from every IP. The EXCEPTION is a genuine rate limit, which IS reported as
    // 429 below: the previous blanket "never 429" was what produced "This is an upstream
    // fault, not a rate limit" on a body reading "FreeUsageLimitError: Rate limit exceeded".
    // A client can only back off correctly if it is told the truth about which it is.
    if (stream) {
      // Text already on the wire means this was a partial answer, not a failure the
      // client could retry invisibly. Counted so the panel stops claiming a clean
      // run while a harness shows a truncated reply.
      if (entry.committed) notePartial(result.kind)
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
          //
          // Name the upstream reason here too. Only the non-streaming path did, so a
          // STREAMING client -- which is every harness -- was told merely
          // "(transport); tried 2 time(s) across lanes" for an upstream 400 reading
          // "Missing required parameter: tools[4].type", and for a 500 reading
          // "provider exploded". The kind alone cannot distinguish a provider outage
          // from a schema the gateway rejected.
          message: (result.kind === 'context' ? contextMessage(result, model) : result.kind === 'gone'
            // Nothing to retry and nothing to wait for: say so instead of
            // reporting an opaque upstream refusal.
            ? `the provider withdrew ${PROVIDER}/${model}; it is no longer available`
            : (result.kind === 'limited' || result.kind === 'ip_blocked' || isRateLimitBody(result))
              // A streaming client cannot see a 429 -- the headers are long gone --
              // so the truth has to be in the message itself. It used to read
              // "no lane could serve this request (limited); tried 1 time(s) across lanes",
              // which is wrong twice over in direct mode: there are no lanes, and it is a
              // quota, not a lane problem.
              ? `${PROVIDER}/${model}: ${limitHeadline(result)}` +
                `${upstreamDetail(result, model) ? ` (${upstreamDetail(result, model)})` : ''}. ` +
                limitAdvice(result) +
                limitResetNote(result, Number(result.retryAfter))
              : `no lane could serve this request (${result.kind}); ` +
              `tried ${entry.attempts || 1} time(s) across ${result.kind === 'empty' ? 'fresh lanes' : 'lanes'}` +
              `${upstreamDetail(result, model) ? `; upstream: ${upstreamDetail(result, model)}` : ''}`),
          type: result.kind === 'context' ? 'invalid_request_error' : 'upstream_unavailable',
          ...(result.kind === 'context' ? { code: 'context_length_exceeded' } : {}),
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
    const detail = upstreamDetail(result, model) || result.kind

    // A genuine rate limit is NOT an upstream fault, and saying so was actively
    // misleading. This exact sentence was appended to every refusal regardless of
    // cause, so a client that was simply out of free-tier quota was told "This is an
    // upstream fault, not a rate limit" while the body said "FreeUsageLimitError:
    // Rate limit exceeded". That sent the operator looking for a code bug for hours.
    //
    // So: a real rate limit gets HTTP 429 and Retry-After, which is both true and what
    // lets a client back off on its own. Everything else keeps 502.
    if (result.kind === 'limited' || result.kind === 'ip_blocked' || isRateLimitBody(result)) {
      const retryAfter = result.retryAfter ?? rateLimitRetryAfter(result)
      // A real daily exhaustion is not "retry in a moment": name the reset so the message
      // is actionable instead of inviting the client to hammer a spent bucket. A gate 429
      // carries the same Retry-After but no window, so it names no time.
      const resetNote = limitResetNote(result, retryAfter)
      return json(res, 429, {
        error: {
          message: `${PROVIDER}/${model}: ${limitHeadline(result)}` +
            `${detail ? ` (${detail})` : ''}. ` +
            limitAdvice(result) +
            resetNote,
          type: 'rate_limit_exceeded',
        },
      }, { 'Retry-After': String(retryAfter) })
    }

    if (result.kind === 'context') {
      // A refusal is evidence about the real window. Lower what we advertise rather than
      // repeating the same over-long prompt at every lane and every reconnection.
      catalogue.noteContextRefusal(model)
      return json(res, 400, {
        error: { message: contextMessage(result, model), type: 'invalid_request_error', code: 'context_length_exceeded' },
      })
    }

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
    const sentIds = entry.streamedToolIds || new Map()
    const repair = []
    for (let i = 0; i < bufferedCalls.length; i++) {
      const c = bufferedCalls[i]
      if (!c) continue
      // Reconcile against the index this call ACTUALLY streamed under, not against
      // its position in the folded array. Those can differ: a lane numbering calls by
      // upstream index (2, 0) folds into a dense array at positions 1 and 0, and
      // comparing position 1 with "what arrived at index 1" reads as a call that was
      // never sent -- so it was restated and the client executed it twice, with the
      // arguments appended twice. Identity is the only reliable link between the two.
      const id = typeof c.id === 'string' ? c.id : ''
      const at = (id && sentIds.has(id)) ? sentIds.get(id) : i
      const fullName = c.function?.name || ''
      const gotName = sentNames.get(at) || ''
      const fullArgs = typeof c.function?.arguments === 'string' ? c.function.arguments : ''
      const got = sent.get(at) || ''
      if (!entry.streamedTools) {
        // Nothing was streamed: send the whole call, name and id included.
        repair.push({ ...c, index: at })
        continue
      }
      // If the client's received name does not match the full folded name, the name
      // was split across batches or truncated in flight. Restate the call in full.
      if (fullName !== gotName) {
        repair.push({ index: at, ...c })
        continue
      }
      // Identity is never repeated: a client appending name fragments would turn
      // "edit" into "editedit". Only the argument tail, when one is missing.
      if (fullArgs.length > got.length && fullArgs.startsWith(got)) {
        repair.push({ index: at, function: { arguments: fullArgs.slice(got.length) } })
      } else if (fullArgs !== got) {
        // The client's copy is not a prefix of the folded call, so appending
        // cannot repair it. Restate the call in full rather than leave a harness
        // with JSON it cannot parse.
        repair.push({ index: at, ...c })
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
<div class=c><b>${clientStats.gone}</b><span>client gone</span></div>
<div class=c><b>${clientStats.partial}</b><span>partial streams</span></div>
<div class=c><b>${Math.round((Date.now() - processStartedAt) / 1000)}s</b><span>uptime</span></div>
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

  // A closed upstream window cannot be served by any lane, so do not hand out work that
  // would spend an upstream call only to report the same reset. The lane keeps its poll
  // (the heartbeat above keeps it in the pool) and is told there is nothing to do.
  if (exhaustion.active()) {
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
      // Which client-facing index carried which call. The finaliser reconciles by
      // identity rather than by array position, so it needs this: a lane can number
      // its calls differently from the order the folded completion ends up in.
      const ids = entry.streamedToolIds || (entry.streamedToolIds = new Map())
      for (const c of body.toolCalls) {
        const i = typeof c.index === 'number' ? c.index : 0
        if (typeof c.id === 'string' && c.id) ids.set(c.id, i)
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
  writer(body.text ?? '', Array.isArray(body.toolCalls) ? body.toolCalls : null,
    typeof body.reasoning === 'string' ? body.reasoning : '')
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
          ok: true, provider: PROVIDER, models: catalogue.count(),
          catalogue: catalogue.status(),
          build: `${config.BUILD_ID} (${config.BUILD_BUILT})`,
          laneCode: laneCodeStats(),
          direct: directStats(),
          escalate: escalateStats(),
          exhaustion: exhaustion.status(),
          rateLimits,
          // Client-side outcomes: a request the eyeball abandoned and a stream that
          // died after commit are invisible in the lane counters, and they are what a
          // harness actually reports as "terminated" / "request timed out".
          clients: { ...clientStats, uptimeS: Math.round((Date.now() - processStartedAt) / 1000) },
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
        return json(res, 200, catalogue.payload(`${config.BUILD_ID} (${config.BUILD_BUILT})`))
      }
      const m = path.match(/^\/v1\/models\/(.+)$/)
      if (method === 'GET' && m) {
        const found = catalogue.find(decodeURIComponent(m[1]))
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
  // cloudflared pools its origin connections and holds an idle one for about 90s.
  // Node's default keepAliveTimeout is 5s, so the connector can pick a socket the
  // relay has just closed: the request never reaches the handler and the client gets
  // a 502 that no relay counter can explain. Keep the origin socket open past the
  // connector's idle window. headersTimeout must stay above it.
  server.keepAliveTimeout = 120000
  server.headersTimeout = 125000
  catalogue.start()
  server.listen(PORT, HOST, () => {
    console.log(`[relay] build ${config.BUILD_ID} built ${config.BUILD_BUILT}`)
    console.log(`[relay] listening on http://${HOST}:${PORT}`)
    console.log(`[relay] client API: /v1/models, /v1/chat/completions  (models: ${catalogue.count()} seed until the gateway list loads)`)
    console.log(`[relay] dashboard:  /dashboard`)
    console.log(`[relay] max lanes ${MAX_LANES}, retry limit ${RETRY_LIMIT}, max hold ${MAX_HOLD_MS}ms`)
  })
  return server
}

if (process.argv[1] && process.argv[1].endsWith('server.js')) start()