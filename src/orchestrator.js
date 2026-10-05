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

async function main () {
  if (!TOKEN || !REPO) {
    console.error('[orch] GH_TOKEN and GH_REPO are required; lane top-up disabled')
    return
  }
  console.log(`[orch] repo=${REPO} workflow=${WORKFLOW} target=${TARGET} every ${TICK_MS / 1000}s`)

  for (;;) {
    try {
      const { active, queued } = await counts()
      const live = active + queued
      const deficit = TARGET - live
      if (deficit > 0) {
        // Dispatch in small batches: GitHub queues the surplus itself, and a
        // burst of 20 dispatches can trip secondary rate limits on the API.
        const batch = Math.min(deficit, 4)
        for (let i = 0; i < batch; i++) {
          try {
            await api(`/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches`, {
              method: 'POST', body: { ref: REF, inputs: {} },
            })
          } catch (e) {
            console.error(`[orch] dispatch failed: ${e.message}`)
            break
          }
          await sleep(1200)
        }
        console.log(`${stamp()} [orch] active=${active} queued=${queued} deficit=${deficit} -> dispatched ${batch}`)
      } else {
        console.log(`${stamp()} [orch] active=${active} queued=${queued} at target`)
      }
    } catch (e) {
      console.error(`${stamp()} [orch] tick error: ${e.message}`)
    }
    await sleep(TICK_MS)
  }
}

main().catch((e) => { console.error('[orch] fatal:', e); process.exit(1) })

export { api, counts }