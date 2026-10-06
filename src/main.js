// Single entry point for the jar.
//
// Starts, in order:
//   1. the relay HTTP server  (client API + lane queue)
//   2. the lane orchestrator  (tops GitHub Actions runs back up to TARGET)
//
// Configuration is resolved in config.js (env > start.properties > baked), so a
// baked PORT is honoured and can still be overridden after upload.

import { config, applyConfigToEnv } from './config.js'
import { start, lanes, queue } from './server.js'

applyConfigToEnv()

const say = (m) => console.log(`[main] ${m}`)

say(`starting: port=${config.PORT} lanes=${config.LANES_TARGET} ` +
  `repo=${config.GH_REPO || '(unset)'} workflow=${config.GH_WORKFLOW}`)

const server = start()

if (config.GH_TOKEN && config.GH_REPO) {
  import('./orchestrator.js')
    .then(() => say('lane orchestrator started'))
    .catch((e) => say(`lane orchestrator failed to load: ${e.message}`))
} else {
  say('no GH_TOKEN/GH_REPO: lane orchestration disabled, relay will hold requests until lanes appear')
}

// If nothing claims queued work, say so rather than looking silently broken.
let lastPending = 0
const supervisor = setInterval(() => {
  const q = queue.stats()
  if (q.pending > 0 && q.pending === lastPending) {
    console.log(`[main] warning: ${q.pending} request(s) queued but not claimed by any lane`)
  }
  lastPending = q.pending
}, 60000)
supervisor.unref?.()

setInterval(() => {
  const l = lanes.stats()
  const q = queue.stats()
  console.log(`[main] lanes=${l.lanes}/${l.maxLanes} served=${l.served} burned=${l.retired} ` +
    `timeouts=${l.timeouts} pending=${q.pending} inflight=${q.inflight} avg=${l.avgMs}ms`)
}, 60000).unref?.()

const shutdown = () => {
  console.log('[main] shutting down')
  clearInterval(supervisor)
  try { server.close() } catch { /* ignore */ }
  process.exit(0)
}
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)