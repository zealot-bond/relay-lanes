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
import { queue, lanes } from './server.js'

const TOKEN = config.GH_TOKEN
const REPO = config.GH_REPO
const WORKFLOW = config.GH_WORKFLOW
const REF = config.GH_REF
const TARGET = config.LANES_TARGET
const TICK_MS = config.ORCH_TICK_MS
// Lanes held warm while the relay is idle. Defaults to TARGET so the pool is
// genuinely full and a request never waits on a cold start. Lower it to cut
// runner minutes when the relay is idle for long stretches.
const STANDBY = config.LANES_STANDBY > 0 ? config.LANES_STANDBY : TARGET
const API = 'https://api.github.com'
const PORT = config.PORT
// The lane needs to reach this relay. Passing it as a dispatch input means the
// jar never has to be told its own public URL, which is the one value that is
// impossible to know reliably from inside a container.
const RELAY_TOKEN = config.RELAY_TOKEN
const PUBLIC_URL = config.RELAY_PUBLIC_URL.replace(/\/$/, '')

// Two polls every tick, for the life of the process. Without keep-alive each one
// paid a fresh TLS handshake, and GitHub's secondary rate limiter counts
// connection churn against the same budget as requests.
const GH_AGENT = new https.Agent({ keepAlive: true, keepAliveMsecs: 15000, maxSockets: 4 })

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const stamp = () => new Date().toISOString().slice(11, 19)

function api (path, { method = 'GET', body } = {}) {
  return new Promise((resolve, reject) => {
    const data = body === undefined ? null : JSON.stringify(body)
    const req = https.request(API + path, {
      method,
      agent: GH_AGENT,
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
        // Consume the body before the socket can be reused.
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

/**
 * Count this workflow's runs.
 *
 * Each status is fetched independently and a failed half degrades to a known
 * value rather than rejecting the pair: with Promise.all a transient 5xx on the
 * queued query failed the whole tick, so nothing was dispatched even though the
 * in-progress count was known. A failed count is reported as UNKNOWN, and the
 * caller then trusts the relay's own lane registry instead of assuming zero --
 * assuming zero is what produced a burst of 20 duplicate runs.
 */
const counts = async () => {
  const one = async (status) => {
    try {
      const r = await api(`/repos/${REPO}/actions/workflows/${WORKFLOW}/runs?status=${status}&per_page=100`)
      const runs = r?.workflow_runs
      if (!Array.isArray(runs)) return null
      return runs.length
    } catch { return null }
  }
  const [inProgress, queued] = await Promise.all([one('in_progress'), one('queued')])
  if (inProgress === null && queued === null) return { active: null, queued: null, known: false }
  return { active: inProgress ?? 0, queued: queued ?? 0, known: true }
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

// How recently a lane must have been seen to count as live. Generous: a lane
// long-polls for 20s at a time and only heartbeats on that cycle.
const LANE_FRESH_MS = config.LANE_FRESH_MS

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
    // Do not dispatch at all. A run with no callback URL registers against
    // 127.0.0.1, throws, and exits 1 -- so every dispatch burns runner minutes
    // and a slice of the hourly budget on a run that provably cannot work.
    console.error('[orch] no relay URL could be determined; NOT dispatching. ' +
      'Set RELAY_PUBLIC_URL in baked-credentials.json')
    return
  }

  // Fail loudly and early if the workflow does not exist in that repo. A 404 here
  // otherwise repeats forever and the pool silently stays empty, which looks
  // exactly like "the code is broken" from the outside.
  let warnedMissing = false

  // Runaway guard. Overnight this dispatched 3212 workflow runs in ~10 hours for a
  // pool of 20 lanes, because every lane exited after 90s idle and each exit
  // looked like a deficit. The brake is now demand-based sizing plus an hourly
  // cap, NOT a cooldown between successful dispatches: the cooldown made a cold
  // start take 30 minutes (one lane per 90s), which is the opposite of what a
  // pool is for. GitHub queues dispatches itself and starts them as concurrency
  // slots free, so a burst is safe and is the fastest path to N lanes.
  const BUDGET_PER_HOUR = config.DISPATCH_BUDGET
  const MAX_BURST = config.DISPATCH_BURST
  const DISPATCH_SPACING_MS = config.DISPATCH_SPACING_MS
  // Only a failing dispatch backs off. A success means the config is right.
  const COOLDOWN_MS = config.DISPATCH_COOLDOWN_MS
  let dispatches = []
  let cooldownUntil = 0

  const budgetLeft = () => {
    const hourAgo = Date.now() - 3600000
    dispatches = dispatches.filter((t) => t > hourAgo)
    return BUDGET_PER_HOUR - dispatches.length
  }

  for (;;) {
    try {
      const { active, queued, known } = await counts()

      // The orchestrator shares a process with the relay, so it reads the real
      // queue. Pending work scales the pool to TARGET; an idle pool settles at
      // STANDBY so runner minutes are not spent on nothing.
      const q = queue.stats()
      const busy = q.pending > 0 || q.inflight > 0
      const target = busy ? TARGET : STANDBY

      // Count capacity as whichever view is HIGHER, because they fail in opposite
      // directions and each failure mode was observed live:
      //
      //   GitHub under-reports: reported active=0 while 20 lanes were registered
      //   and serving -> 60 wasted dispatches in four minutes.
      //
      //   lanes under-reports: immediately after a relay restart the registry is
      //   empty while 100+ runs are already queued -> the relay fired 20 more per
      //   tick, pushing the queue past 120.
      //
      // A queued run is a lane that will exist, so max() is the safe reading: it
      // dispatches only when neither view shows a pool.
      const liveLanes = lanes.liveLanes(LANE_FRESH_MS).length

      // When the GitHub API could not be reached at all, its counts are unknown,
      // not zero. Treating unknown as zero is the restart flood: trust only the
      // relay's own registry for this tick and record the API as unavailable.
      if (!known) {
        console.error(`${stamp()} [orch] github run counts unavailable; using lane registry only ` +
          `(lanes=${liveLanes})`)
      }
      const capacity = known ? Math.max(liveLanes, active + queued) : liveLanes
      const deficit = target - capacity
      const left = budgetLeft()

      if (left <= 0) {
        console.log(`${stamp()} [orch] lanes=${liveLanes} github=${active}+${queued} want=${target} ` +
          `pending=${q.pending} but hourly dispatch budget spent ` +
          `(${dispatches.length}/${BUDGET_PER_HOUR}) -- holding off`)
      } else if (Date.now() < cooldownUntil) {
        const wait = Math.ceil((cooldownUntil - Date.now()) / 1000)
        console.log(`${stamp()} [orch] lanes=${liveLanes} github=${active}+${queued} want=${target} ` +
          `pending=${q.pending} cooling down ${wait}s`)
      } else if (deficit > 0) {
        // Fire the whole deficit at once, spaced just enough to avoid the API's
        // secondary rate limits. A cold start goes from ~30 minutes to seconds.
        const want = Math.min(deficit, MAX_BURST, left)
        let sent = 0
        for (let i = 0; i < want; i++) {
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
            sent++
          } catch (e) {
            if (/404/.test(e.message) && !warnedMissing) {
              warnedMissing = true
              console.error(`[orch] WORKFLOW NOT FOUND: ${WORKFLOW} in ${REPO}.`)
              console.error('[orch] lanes cannot start. Check GH_REPO and GH_WORKFLOW in baked-credentials.json.')
            }
            console.error(`[orch] dispatch failed after ${sent}: ${e.message}`)
            // Back off hard: a failing dispatch means the config is wrong, and
            // hammering it is what produced thousands of useless runs.
            cooldownUntil = Date.now() + COOLDOWN_MS * 4
            break
          }
          if (i < want - 1) await sleep(DISPATCH_SPACING_MS)
        }
        if (sent) {
          console.log(`${stamp()} [orch] lanes=${liveLanes} github=${active}+${queued} ` +
            `want=${target} deficit=${deficit} -> dispatched ${sent} ` +
            `(${dispatches.length}/${BUDGET_PER_HOUR} this hour)`)
        }
      } else {
        console.log(`${stamp()} [orch] lanes=${liveLanes} github=${active}+${queued} ` +
          `capacity=${capacity} at target (${target})`)
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