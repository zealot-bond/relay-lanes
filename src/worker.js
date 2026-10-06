// GitHub Actions worker: registers as a lane, then claims work until it is told
// to stop or its egress IP reports exhaustion.

import { callUpstream, destroyKeepalive } from './lane.js'

const RELAY = (process.env.RELAY_URL || 'http://127.0.0.1:8791').replace(/\/$/, '')
const TOKEN = process.env.RELAY_TOKEN || 'relay'
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
const DELTA_BATCH_MS = Number(process.env.DELTA_BATCH_MS || 40)

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
  })
  if (!reg?.ok) {
    if (reg?.error === 'lane capacity reached') {
      // Not a failure. The pool is full, so this runner has no work to do and
      // exiting immediately avoids burning runner minutes for nothing.
      console.log('[lane] relay is at lane capacity; nothing to do')
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
      const r = await post('/lane/register', { laneId: LANE_ID, rejoin: true }).catch(() => null)
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
    let streamedAny = false
    const flushDelta = async () => {
      if (deltaTimer) { clearTimeout(deltaTimer); deltaTimer = null }
      const hasText = pending.length > 0
      const hasTools = pendingTools.length > 0
      if (!hasText && !hasTools) return
      const body = { laneId: LANE_ID, entryId: claim.entryId }
      if (hasText) body.text = pending.join('')
      if (hasTools) body.toolCalls = pendingTools
      pending = []
      pendingTools = []
      const r = await post('/lane/delta', body).catch(() => null)
      if (r?.ok) streamedAny = true
    }
    const schedule = () => {
      if (!deltaTimer) deltaTimer = setTimeout(flushDelta, DELTA_BATCH_MS)
    }

    const result = await callUpstream(claim.job, {
      // Only stream when the client asked for a stream. Sending deltas for a
      // non-streaming request would just be a rejected POST per batch.
      onDelta: claim.job.stream
        ? (text) => {
            pending.push(text)
            schedule()
          }
        : undefined,
      // Tool-call deltas must reach an agent harness exactly like text does:
      // it reassembles the call from these to know which tool to run.
      onToolDelta: claim.job.stream
        ? (deltas) => {
            pendingTools.push(...deltas)
            schedule()
          }
        : undefined,
    })
    await flushDelta()

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
        // Tells the relay the client already received the text through
        // /lane/delta, so it must not replay the buffered completion.
        streamed: streamedAny,
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