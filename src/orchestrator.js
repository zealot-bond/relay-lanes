// Keeps a steady pool of lanes by topping up GitHub Actions workflow runs.
//
// Why this shape: a free GitHub account allows 20 concurrent jobs, and queued
// runs start automatically as running ones finish. So "always have TARGET lanes"
// reduces to "always have TARGET runs queued or in progress", and GitHub does the
// scheduling and the handoff for free.
//
// A lane that retires (worker exit 75 = its egress bucket is spent) simply frees
// a concurrency slot, and the next dispatch fills it with a fresh runner that has
// a fresh IP. No central bookkeeping of which IP is alive is needed: the relay's
// own lane registry is the source of truth for liveness.

import https from 'node:https'
import { config } from './config.js'
import { queue } from './server.js'

const TOKEN = config.GH_TOKEN
const REPO = config.GH_REPO
const WORKFLOW = config.GH_WORKFLOW
const REF = config.GH_REF
const TARGET = config.LANES_TARGET
const TICK_MS = config.ORCH_TICK_MS
// Lanes kept warm while the relay is idle: enough to absorb a burst instantly,
// without paying for a full pool around an empty queue.
const STANDBY = Number(process.env.LANES_STANDBY || 4)
const API = 'https://api.github.com'
const PORT = config.PORT
// The lane needs to reach this relay. Passing it as a dispatch input means the
// jar never has to be told its own public URL, which is the one value that is
// impossible to know reliably from inside a container.
const RELAY_TOKEN = config.RELAY_TOKEN
const PUBLIC_URL = config.RELAY_PUBLIC_URL.replace(/\/$/, '')

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const stamp = () => new Date().toISOString().slice(11, 19)

function api (path, { method = 'GET', body } = {}) {
  return new Promise((resolve, reject) => {
    const data = body === undefined ? null : JSON.stringify(body)
    const req = https.request(API + path, {
      method,
      headers: {
        Accept: 'application/vnd.github+json',
        'User-Agent': 'relay-orchestrator',
        Authorization: `Bearer ${TOKEN}`,
        ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {}),
      },
      timeout: 30000,
    }, (res) => {
      let raw = ''
      res.on('data', (c) => { raw += c; })
      res.on('end', () => {
        if (res.statusCode >= 300) return reject(new Error(`HTTP ${res.statusCode} ${raw.slice(0, 160)}`))
        try { resolve(raw ? JSON.parse(raw) : null) } catch (e) { reject(e) }
      })
    })
    req.on('error', reject)
    req.setTimeout(30000, () => req.destroy(new Error('timeout')))
    if (data) req.write(data)
    req.end()
  })
}

const counts = async () => {
  const [inProgress, queued] = await Promise.all([
    api(`/repos/${REPO}/actions/workflows/${WORKFLOW}/runs?status=in_progress&per_page=100`),
    api(`/repos/${REPO}/actions/workflows/${WORKFLOW}/runs?status=queued&per_page=100`),
  ])
  return { active: inProgress?.workflow_runs?.length ?? 0, queued: queued?.workflow_runs?.length ?? 0 }
}

/**
 * Work out the URL lanes should call back on.
 *
 * Order: an explicit RELAY_PUBLIC_URL wins, then whatever the panel put in
 * PANEL_URL, then a public-IP lookup combined with the listening port. The
 * lookup is done once and cached because it is the only outbound call needed.
 */
async function resolveRelayUrl () {
  if (PUBLIC_URL) return PUBLIC_URL
  const fromEnv = (process.env.PANEL_URL || '').replace(/\/$/, '')
  if (fromEnv) return fromEnv
  try {
    const res = await fetch('https://api.ipify.org', { signal: AbortSignal.timeout(8000) })
    const ip = (await res.text()).trim()
    if (/^\d+\.\d+\.\d+\.\d+$/.test(ip)) {
      const url = `http://${ip}:${PORT}`
      console.log(`[orch] discovered relay url ${url}`)
      return url
    }
  } catch { /* fall through */ }
  return ''
}

async function main () {
  if (!TOKEN || !REPO) {
    console.error('[orch] GH_TOKEN and GH_REPO are required; lane top-up disabled')
    return
  }
  const relayUrl = await resolveRelayUrl()
  console.log(`[orch] repo=${REPO} workflow=${WORKFLOW} target=${TARGET} standby=${STANDBY} ` +
    `every ${TICK_MS / 1000}s`)
  console.log(`[orch] lanes will call back on ${relayUrl || '(unknown: dispatching without a url!)'}`)
  if (!relayUrl) {
    console.error('[orch] no relay URL could be determined; set RELAY_PUBLIC_URL in baked-credentials.json')
  }

  // Fail loudly and early if the workflow does not exist in that repo. A 404 here
  // otherwise repeats forever and the pool silently stays empty, which looks
  // exactly like "the code is broken" from the outside.
  let warnedMissing = false

  // Runaway guard. Overnight this dispatched 3212 workflow runs in ~10 hours for a
  // pool of 20 lanes, because every lane exited after 90s idle and each exit
  // looked like a deficit. A free account has a monthly minutes budget, so an
  // unbounded dispatch loop is an account-level risk, not just noise.
  //
  // Three brakes, each independent:
  //   DISPATCH_BUDGET  hard cap on dispatches per hour
  //   cooldown        a run that fails fast must not be retried immediately
  //   single-per-tick  one dispatch per tick, so a refill takes minutes not seconds
  const BUDGET_PER_HOUR = Number(process.env.DISPATCH_BUDGET || 120)
  const COOLDOWN_MS = Number(process.env.DISPATCH_COOLDOWN_MS || 90000)
  let dispatches = []
  let cooldownUntil = 0

  const budgetLeft = () => {
    const hourAgo = Date.now() - 3600000
    dispatches = dispatches.filter((t) => t > hourAgo)
    return BUDGET_PER_HOUR - dispatches.length
  }

  for (;;) {
    try {
      const { active, queued } = await counts()

      // Demand-driven sizing. The orchestrator shares a process with the relay,
      // so it can read the real queue instead of guessing. Holding a full pool of
      // TARGET lanes around an idle relay is what burned the account overnight
      // (3212 runs in ~10h), because runners are charged by the minute whether
      // or not they are used. A small standby pool absorbs bursts; the rest is
      // spun up only when there is work waiting for it.
      const q = queue.stats()
      const busy = q.pending > 0 || q.inflight > 0
      const target = busy ? TARGET : STANDBY
      const live = active + queued
      const deficit = target - live
      const left = budgetLeft()

      if (left <= 0) {
        console.log(`${stamp()} [orch] active=${live} want=${target} pending=${q.pending} ` +
          `but hourly dispatch budget spent (${dispatches.length}/${BUDGET_PER_HOUR}) -- holding off`)
      } else if (Date.now() < cooldownUntil) {
        const wait = Math.ceil((cooldownUntil - Date.now()) / 1000)
        console.log(`${stamp()} [orch] active=${live} want=${target} pending=${q.pending} cooling down ${wait}s`)
      } else if (deficit > 0) {
        // Exactly one dispatch per tick. GitHub queues the surplus and starts it
        // as slots free, so a burst buys nothing but quota risk.
        try {
          // Pass the callback URL and token as inputs so a lane never depends
          // on repo secrets being configured correctly.
          await api(`/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches`, {
            method: 'POST',
            body: {
              ref: REF,
              inputs: { relay_url: relayUrl, relay_token: RELAY_TOKEN },
            },
          })
          dispatches.push(Date.now())
          cooldownUntil = Date.now() + COOLDOWN_MS
          console.log(`${stamp()} [orch] active=${active} queued=${queued} deficit=${deficit} ` +
            `-> dispatched 1 (${dispatches.length}/${BUDGET_PER_HOUR} this hour)`)
        } catch (e) {
          if (/404/.test(e.message) && !warnedMissing) {
            warnedMissing = true
            console.error(`[orch] WORKFLOW NOT FOUND: ${WORKFLOW} in ${REPO}.`)
            console.error('[orch] lanes cannot start. Check GH_REPO and GH_WORKFLOW in baked-credentials.json.')
          }
          console.error(`[orch] dispatch failed: ${e.message}`)
          // Back off hard: a failing dispatch means the config is wrong, and
          // hammering it is what produced thousands of useless runs.
          cooldownUntil = Date.now() + COOLDOWN_MS * 4
        }
      } else {
        console.log(`${stamp()} [orch] active=${active} queued=${queued} at target`)
      }
    } catch (e) {
      if (/404/.test(e.message) && !warnedMissing) {
        warnedMissing = true
        console.error(`[orch] WORKFLOW NOT FOUND: ${WORKFLOW} in ${REPO} -- lanes cannot start.`)
      }
      console.error(`${stamp()} [orch] tick error: ${e.message}`)
    }
    await sleep(TICK_MS)
  }
}

main().catch((e) => { console.error('[orch] fatal:', e); process.exit(1) })

export { api, counts }