// GitHub Actions worker: registers as a lane, then claims work until it is told
// to stop or its egress IP reports exhaustion.

import { callUpstream, destroyKeepalive } from './lane.js'
import { mergeToolDeltas } from './toolmerge.js'
import { CODE_VERSION } from './codeversion.js'
import { config } from './config.js'

const RELAY = (process.env.RELAY_URL || 'http://127.0.0.1:8791').replace(/\/$/, '')
// The relay token is the one value the lane cannot get from baked config: the
// orchestrator passes it as a dispatch input, so env wins here.
const TOKEN = process.env.RELAY_TOKEN || config.RELAY_TOKEN
const LANE_ID = process.env.LANE_ID ||
  `runner-${process.env.GITHUB_RUN_ID || 'local'}-${process.env.GITHUB_RUN_ATTEMPT || '0'}`
// A lane should live until its egress bucket is spent (exit 75) or it genuinely
// breaks, not because a wall-clock timer ran out. The old 540s lifetime made
// every runner quit while its bucket still had capacity, which forced a
// replacement runner -- new IP, cold caches, re-registered lane -- for no reason.
// This is set below the workflow's own timeout so the JOB ends the lane, not the
// other way round, and the exit is still a clean 0.
const MAX_LIFETIME_S = Number(process.env.LANE_LIFETIME_S || 3300)
// Idle lanes park rather than churn. Short idle exits were what turned into
// thousands of workflow runs overnight.
const IDLE_EXIT_S = Number(process.env.LANE_IDLE_EXIT_S || 2400)
// Fragments are batched before being forwarded to the relay. One HTTP call per
// token would cost more than the tokens themselves; a short window keeps
// time-to-first-token near upstream latency while collapsing a burst of tokens
// into one request.
const DELTA_BATCH_MS = config.DELTA_BATCH_MS


const H = { 'Content-Type': 'application/json', 'x-relay-token': TOKEN }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const post = async (path, body, method = 'POST') => {
  const res = await fetch(RELAY + path, {
    method,
    headers: H,
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (res.status === 204) return null
  const text = await res.text()
  try { return JSON.parse(text) } catch { return { raw: text } }
}

async function main () {
  // Register BEFORE the first claim: the relay rejects claims from unregistered
  // lanes, which is the guard that stops a retired lane from working again.
  const reg = await post('/lane/register', {
    laneId: LANE_ID,
    runnerId: process.env.GITHUB_RUN_ID,
    attempt: process.env.GITHUB_RUN_ATTEMPT,
    repo: process.env.GITHUB_REPOSITORY,
    // Lets the relay tell which code this runner is executing. Runners check out a
    // git repo while the relay ships in a jar, so the two drift apart silently.
    codeVersion: CODE_VERSION,
  })
  if (!reg?.ok) {
    if (reg?.error === 'lane capacity reached') {
      // Not a failure. The pool is full, so this runner has no work to do and
      // exiting immediately avoids burning runner minutes for nothing.
      console.log('[lane] relay is at lane capacity; nothing to do')
      destroyKeepalive()
      process.exit(0)
    }
    if (reg?.error === 'lane retired') {
      // The relay remembers this lane id as spent. Never re-enter: a retired
      // egress IP can only answer 429, and rejoining would burn runner minutes
      // rediscovering that.
      console.log('[lane] this lane id was retired (bucket spent); exiting')
      destroyKeepalive()
      process.exit(0)
    }
    console.error('[lane] registration failed:', JSON.stringify(reg))
    process.exit(1)
  }
  console.log(`[lane] registered ${LANE_ID} (queue depth ${reg.queueDepth})`)

  const started = Date.now()
  let served = 0, limited = 0, failed = 0, timeouts = 0, empty = 0
  let lastWork = Date.now()
  let claimFails = 0

  while ((Date.now() - started) / 1000 < MAX_LIFETIME_S) {
    let claim
    try {
      claim = await post(`/lane/claim?laneId=${encodeURIComponent(LANE_ID)}&wait=20000`, undefined, 'GET')
      claimFails = 0
    } catch (e) {
      // A lane that loses the relay is not a dead lane: the relay restarts, or the
      // panel's proxy resets an idle long-poll. Keep the lane alive and back off,
      // instead of exiting or logging every 2 seconds for the rest of the job.
      claimFails++
      const wait = Math.min(15000, 1000 * 2 ** Math.min(claimFails, 4))
      if (claimFails === 1 || claimFails % 5 === 0) {
        console.log(`[lane] claim failed (${claimFails}): ${e.message} -- retrying in ${wait}ms`)
      }
      await sleep(wait)
      continue
    }

    // Re-register if the relay forgot us (restart wipes the registry). Without
    // this a relay restart silently idles every lane until the job times out.
    if (claim?.error === 'lane not registered') {
      console.log('[lane] relay forgot this lane, re-registering')
      const r = await post('/lane/register', { laneId: LANE_ID, rejoin: true, codeVersion: CODE_VERSION }).catch(() => null)
      if (r?.error === 'lane retired') {
        // Retired while we were away: this runner's egress bucket is spent, so
        // there is nothing left to do with it. Exiting lets the orchestrator
        // start a fresh runner with a fresh IP.
        console.log('[lane] relay retired this lane while disconnected; exiting')
        destroyKeepalive()
        process.exit(0)
      }
      if (!r?.ok) await sleep(3000)
      continue
    }

    if (!claim || !claim.entryId) {
      // Empty queue. Do not exit on the first idle poll: work requeued by a lane
      // that just burned out arrives a moment later.
      if ((Date.now() - lastWork) / 1000 > IDLE_EXIT_S) {
        console.log(`[lane] idle ${IDLE_EXIT_S}s, exiting to free a concurrency slot`)
        break
      }
      continue
    }
    lastWork = Date.now()

    // Fragments are batched before being forwarded to the relay. One HTTP call
    // per token would cost more than the tokens themselves; a short window keeps
    // time-to-first-token near upstream latency while collapsing a burst of
    // tokens into one request.
    let pending = []
    let pendingTools = []
    let deltaTimer = null
    let streamedText = false
    let streamedTools = false
    let streamedTextChars = 0
    // Survives every flush of this claim: identity arrives in one 40ms window and
    // arguments in the next, so a per-batch fold would drop the id/name.
    const toolAcc = new Map()
    // Flushes are chained, never concurrent. Two overlapping POSTs can be
    // reordered in flight, and a batch that lands after a later one interleaves
    // the client's text -- the exact "duplicated / scrambled output" symptom.
    let flushChain = Promise.resolve()
    const doFlush = async () => {
      const hasText = pending.length > 0
      const hasTools = pendingTools.length > 0
      if (!hasText && !hasTools) return
      const body = { laneId: LANE_ID, entryId: claim.entryId }
      if (hasText) body.text = pending.join('')
      if (hasTools) body.toolCalls = mergeToolDeltas(pendingTools, toolAcc)
      pending = []
      pendingTools = []
      const r = await post('/lane/delta', body).catch(() => null)
      if (r?.ok) {
        // Tracked separately: a turn can stream tool calls and no text, and the
        // relay must not treat that as "the client already saw everything".
        if (hasText) {
          streamedText = true
          streamedTextChars += body.text.length
        }
        if (hasTools) streamedTools = true
      }
    }
    const flushDelta = () => {
      if (deltaTimer) { clearTimeout(deltaTimer); deltaTimer = null }
      flushChain = flushChain.then(doFlush).catch(() => {})
      return flushChain
    }
    const schedule = () => {
      if (!deltaTimer) deltaTimer = setTimeout(() => { flushDelta() }, DELTA_BATCH_MS)
    }

    // A lane can legitimately be busy for minutes on a large prompt. Without this it
    // stops heartbeating, the relay stops counting it as live, and it gets pruned
    // as dead while it is actually working -- which shrinks the pool exactly when
    // load is high.
const busyHeartbeat = setInterval(() => {
  post('/lane/heartbeat', { laneId: LANE_ID }).catch(() => {})
}, 20000)
busyHeartbeat.unref?.()

    // The result POST is the last thing this claim does, and it must still happen
    // if callUpstream throws: an unhandled throw here would skip the report
    // entirely, leaving the relay to serve out the full ATTEMPT_HARD_MS before it
    // gives up on work that had already finished.
    let result
    try {
      result = await callUpstream(claim.job, {
        // Only stream when the client asked for a stream. Sending deltas for a
        // non-streaming request would just be a rejected POST per batch.
        onDelta: claim.job.stream
          ? (text) => {
              pending.push(text)
              schedule()
            }
          : undefined,
        onToolCall: () => {
          // If a tool call arrives:
          // In self mode or when client provided no tools, withhold pre-tool text
          // that hasn't been flushed yet so the client doesn't see "I'll read that file"
          if (config.TOOL_MODE === 'self' || !claim.job?.tools?.length) {
            pending = []
            if (deltaTimer && !pendingTools.length) {
              clearTimeout(deltaTimer)
              deltaTimer = null
            }
          }
        },
        // Tool-call deltas must reach an agent harness exactly like text does:
        // it reassembles the call from these to know which tool to run.
        onToolDelta: claim.job.stream
          ? (deltas) => {
              pendingTools.push(...deltas)
              schedule()
            }
          : undefined,
      })
    } catch (e) {
      result = { kind: 'error', status: 0, ms: 0, data: null, raw: String(e?.message || e) }
      console.error('[lane] callUpstream threw:', e?.stack || e)
    }
    await flushDelta()
    clearInterval(busyHeartbeat)

    if (result.kind === 'ok') served++
    else if (result.kind === 'limited') limited++
    else if (result.kind === 'timeout') timeouts++
    else if (result.kind === 'empty') empty++
    else failed++

    console.log(`[lane] ${claim.job?.model} -> ${result.kind} (${result.ms}ms)`)

    await post('/lane/result', {
      laneId: LANE_ID,
      entryId: claim.entryId,
      result: {
        kind: result.kind,
        status: result.status,
        ms: result.ms,
        // Tells the relay which parts of the answer the client already received
        // through /lane/delta, so it never replays them. Kept as two flags because
        // a tool-only turn streams no text and must still send its folded calls.
        streamed: streamedText || streamedTools,
        streamedText,
        streamedTools,
        streamedTextLen: streamedTextChars,
        raw: result.kind === 'ok' ? '' : String(result.raw || '').slice(0, 400),
        data: result.data,
      },
    })

    if (result.kind === 'limited') {
      // This egress IP is spent. Exit so the orchestrator starts a replacement
      // runner, which gets a fresh bucket.
      console.log(`[lane] bucket exhausted after ${served} served; retiring this lane`)
      console.log(`[lane] totals served=${served} limited=${limited} timeouts=${timeouts} empty=${empty} failed=${failed}`)
      destroyKeepalive()
      process.exit(75)   // distinctive: burned IP, not a crash
    }
  }

  console.log(`[lane] lifetime done served=${served} limited=${limited} timeouts=${timeouts} empty=${empty} failed=${failed}`)
  destroyKeepalive()
  process.exit(0)
}

main().catch((e) => {
  console.error('[lane] fatal:', e?.stack || e)
  process.exit(1)
})