// Single entry point for the jar.
//
// Starts, in order:
//   1. the relay HTTP server  (client API + lane queue)
//   2. the lane orchestrator  (tops GitHub Actions runs back up to TARGET)
//   3. a supervisor that restarts either if it dies
//
// The orchestrator is optional: if no GitHub token is configured the relay still
// serves, it just has no lanes and holds client requests instead of failing them.

import fs from 'node:fs'
import path from 'node:path'
import { start, lanes, queue } from './server.js'

const BAKED = path.resolve(process.cwd(), 'baked-credentials.json')

const loadBaked = () => {
  try { return JSON.parse(fs.readFileSync(BAKED, 'utf-8')) || {} } catch { return {} }
}

const baked = (k) => {
  const v = process.env[k]
  if (typeof v === 'string' && v.length > 0) return v
  const b = loadBaked()
  return typeof b[k] === 'string' ? b[k] : ''
}

// Push baked values into the environment so the existing modules read them
// without knowing where they came from.
for (const k of ['GH_TOKEN', 'GH_REPO', 'GH_WORKFLOW', 'GH_REF', 'LANES_TARGET',
  'RELAY_TOKEN', 'PORT', 'HOST', 'MAX_LANES', 'MAX_HOLD_MS', 'RETRY_LIMIT',
  'ORCH_TICK_MS']) {
  const v = baked(k)
  if (v && !process.env[k]) process.env[k] = v
}

const say = (m) => console.log(`[main] ${m}`)

say(`starting: port=${process.env.PORT || 8791} lanes=${process.env.LANES_TARGET || 20} ` +
  `repo=${process.env.GH_REPO || '(unset)'} workflow=${process.env.GH_WORKFLOW || 'lane.yml'}`)

const server = start()

let orchestrator = null
if (process.env.GH_TOKEN && process.env.GH_REPO) {
  orchestrator = import('./orchestrator.js').then((m) => m).then(
    () => say('lane orchestrator started'),
    (e) => { say(`lane orchestrator failed to load: ${e.message}`); orchestrator = null },
  )
} else {
  say('no GH_TOKEN/GH_REPO: lane orchestration disabled, relay will hold requests until lanes appear')
}

// Supervisor: keep the queue draining and lanes topped up. A stalled lane shows
// up as work that is queued but never claimed, which is the signal to re-dispatch.
let lastPending = 0
const supervisor = setInterval(() => {
  const q = queue.stats()
  if (q.pending > 0 && q.pending === lastPending) {
    // No progress for a full tick with work waiting: lanes are not claiming.
    // Logged so it is visible on the panel console rather than silent.
    console.log(`[main] warning: ${q.pending} request(s) queued but not being claimed by any lane`)
  }
  lastPending = q.pending
}, 60000)
supervisor.unref?.()

const shutdown = () => {
  console.log('[main] shutting down')
  clearInterval(supervisor)
  try { server.close() } catch { /* ignore */ }
  process.exit(0)
}
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)

setInterval(() => {
  const l = lanes.stats()
  const q = queue.stats()
  console.log(`[main] lanes=${l.lanes}/${l.maxLanes} served=${l.served} burned=${l.retired} ` +
    `timeouts=${l.timeouts} pending=${q.pending} inflight=${q.inflight} avg=${l.avgMs}ms`)
}, 60000).unref?.()