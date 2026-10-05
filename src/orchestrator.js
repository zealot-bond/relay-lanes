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

const TOKEN = process.env.GH_TOKEN || ''
const REPO = process.env.GH_REPO || ''
const WORKFLOW = process.env.GH_WORKFLOW || 'lane.yml'
const REF = process.env.GH_REF || 'main'
const TARGET = Number(process.env.LANES_TARGET || 20)
const TICK_MS = Number(process.env.ORCH_TICK_MS || 20000)
const API = 'https://api.github.com'
const PORT = Number(process.env.PORT || 8791)
// The lane needs to reach this relay. Passing it as a dispatch input means the
// jar never has to be told its own public URL, which is the one value that is
// impossible to know reliably from inside a container.
const RELAY_TOKEN = process.env.RELAY_TOKEN || ''
const PUBLIC_URL = (process.env.RELAY_PUBLIC_URL || '').replace(/\/$/, '')

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
  console.log(`[orch] repo=${REPO} workflow=${WORKFLOW} target=${TARGET} every ${TICK_MS / 1000}s`)
  console.log(`[orch] lanes will call back on ${relayUrl || '(unknown: dispatching without a url!)'}`)
  if (!relayUrl) {
    console.error('[orch] no relay URL could be determined; set RELAY_PUBLIC_URL in baked-credentials.json')
  }

  // Fail loudly and early if the workflow does not exist in that repo. A 404 here
  // otherwise repeats forever and the pool silently stays empty, which looks
  // exactly like "the code is broken" from the outside.
  let warnedMissing = false
  for (;;) {
    try {
      const { active, queued } = await counts()
      const live = active + queued
      const deficit = TARGET - live
      if (deficit > 0) {
        // Dispatch in small batches: GitHub queues the surplus itself, and a
        // burst of 20 dispatches can trip secondary rate limits on the API.
        const batch = Math.min(deficit, 4)
        let sent = 0
        for (let i = 0; i < batch; i++) {
          try {
            // Pass the callback URL and token as inputs so a lane never depends
            // on repo secrets being configured correctly.
            await api(`/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches`, {
              method: 'POST',
              body: {
                ref: REF,
                inputs: {
                  relay_url: relayUrl,
                  relay_token: RELAY_TOKEN,
                },
              },
            })
            sent++
          } catch (e) {
            if (/404/.test(e.message) && !warnedMissing) {
              warnedMissing = true
              console.error(`[orch] WORKFLOW NOT FOUND: ${WORKFLOW} in ${REPO}.`)
              console.error('[orch] lanes cannot start. Check GH_REPO and GH_WORKFLOW in baked-credentials.json.')
            }
            console.error(`[orch] dispatch failed: ${e.message}`)
            break
          }
          await sleep(1200)
        }
        if (!warnedMissing) {
          console.log(`${stamp()} [orch] active=${active} queued=${queued} deficit=${deficit} -> dispatched ${sent}`)
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