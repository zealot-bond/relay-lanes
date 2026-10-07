// Single entry point for the jar.
//
// Starts, in order:
//   1. the relay HTTP server  (client API + lane queue)
//   2. the lane orchestrator  (tops GitHub Actions runs back up to TARGET)
//
// Configuration is resolved in config.js (env > start.properties > baked), so a
// baked PORT is honoured and can still be overridden after upload.

import { config, applyConfigToEnv } from './config.js'

// Applied before server.js/orchestrator.js are LOADED (the dynamic imports below
// run after this statement), so every module sees the same resolved values that
// config.js itself used. A static import would be hoisted above this call and any
// module reading process.env at import time would see the un-baked value.
applyConfigToEnv()

const say = (m) => console.log(`[main] ${m}`)

say(`build=${config.BUILD_ID} built=${config.BUILD_BUILT}`)
say(`starting: port=${config.PORT} lanes=${config.LANES_TARGET} ` +
  `repo=${config.GH_REPO || '(unset)'} workflow=${config.GH_WORKFLOW}`)

const { start, lanes, queue, laneCodeStats } = await import('./server.js')

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
  // Forget lanes that stopped reporting, not only on registration. Otherwise the
  // dashboard and /health keep counting runners that are long gone, and a dead
  // lane holds its slot until the next register call happens to prune it.
  lanes.prune()
  const l = lanes.stats()
  const q = queue.stats()
  const c = laneCodeStats()
  // Anything other than "all current" is the drift that hid a bug for days, so it is
  // printed in the line an operator already reads, not only in /health.
  const code = (c.stale || c.unreported)
    ? ` code=${c.current}ok/${c.stale}STALE/${c.unreported}unversioned(want ${c.expected})`
    : ` code=all-current(${c.expected})`
  console.log(`[main] lanes=${l.lanes}/${l.maxLanes} served=${l.served} burned=${l.retired} ` +
    `timeouts=${l.timeouts} pending=${q.pending} inflight=${q.inflight} avg=${l.avgMs}ms${code}`)
}, 60000).unref?.()

const shutdown = () => {
  console.log('[main] shutting down')
  clearInterval(supervisor)
  try { server.close() } catch { /* ignore */ }
  process.exit(0)
}
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)