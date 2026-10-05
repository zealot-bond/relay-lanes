// GitHub Actions worker: registers as a lane, then claims work until it is told
// to stop or its egress IP reports exhaustion.

import { callUpstream, destroyKeepalive } from './lane.js'

const RELAY = (process.env.RELAY_URL || 'http://127.0.0.1:8791').replace(/\/$/, '')
const TOKEN = process.env.RELAY_TOKEN || 'relay'
const LANE_ID = process.env.LANE_ID ||
  `runner-${process.env.GITHUB_RUN_ID || 'local'}-${process.env.GITHUB_RUN_ATTEMPT || '0'}`
const MAX_LIFETIME_S = Number(process.env.LANE_LIFETIME_S || 270)
// Exit a little earlier than the job timeout so the run exits cleanly instead of
// being killed, which would leave the claim to expire.
const IDLE_EXIT_S = Number(process.env.LANE_IDLE_EXIT_S || 100)

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
    console.error('[lane] registration failed:', JSON.stringify(reg))
    process.exit(1)
  }
  console.log(`[lane] registered ${LANE_ID} (queue depth ${reg.queueDepth})`)

  const started = Date.now()
  let served = 0, limited = 0, failed = 0, timeouts = 0
  let lastWork = Date.now()

  while ((Date.now() - started) / 1000 < MAX_LIFETIME_S) {
    let claim
    try {
      claim = await post(`/lane/claim?laneId=${encodeURIComponent(LANE_ID)}&wait=20000`, undefined, 'GET')
    } catch (e) {
      console.error('[lane] claim failed:', e.message)
      await sleep(2000)
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

    const result = await callUpstream(claim.job)
    if (result.kind === 'ok') served++
    else if (result.kind === 'limited') limited++
    else if (result.kind === 'timeout') timeouts++
    else failed++

    console.log(`[lane] ${claim.job?.model} -> ${result.kind} (${result.ms}ms)`)

    await post('/lane/result', {
      laneId: LANE_ID,
      entryId: claim.entryId,
      result: {
        kind: result.kind,
        status: result.status,
        ms: result.ms,
        raw: result.kind === 'ok' ? '' : String(result.raw || '').slice(0, 400),
        data: result.data,
      },
    })

    if (result.kind === 'limited') {
      // This egress IP is spent. Exit so the orchestrator starts a replacement
      // runner, which gets a fresh bucket.
      console.log(`[lane] bucket exhausted after ${served} served; retiring this lane`)
      console.log(`[lane] totals served=${served} limited=${limited} timeouts=${timeouts} failed=${failed}`)
      destroyKeepalive()
      process.exit(75)   // distinctive: burned IP, not a crash
    }
  }

  console.log(`[lane] lifetime done served=${served} limited=${limited} timeouts=${timeouts} failed=${failed}`)
  destroyKeepalive()
  process.exit(0)
}

main().catch((e) => {
  console.error('[lane] fatal:', e?.stack || e)
  process.exit(1)
})