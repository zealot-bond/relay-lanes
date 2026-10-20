// Direct mode: the relay calls the upstream itself, in-process, with no lane pool.
//
//   DIRECT_MODE=1 java -jar server.jar
//   DIRECT_MODE=1 node src/main.js
//
// Why this exists at all: every GitHub runner in this system exists to provide a distinct
// egress IP, because the free tier's rate limit is keyed on IP. For ONE person on ONE
// machine that buys nothing -- the same bucket either way -- so the whole lane apparatus
// (orchestrator, queue, claims, heartbeats, 20 CI runners) is pure overhead. This runs the
// identical request path with one hop instead of three.
//
// It reuses lane.js's callUpstream verbatim, so request construction, both protocol
// dialects, the SSE fold, tool-call merging and classification are all the same code the
// runners execute. Only the transport to the relay is gone.
//
// Deliberate limits:
//   * DIRECT_MAX_CONCURRENT in-flight upstream calls. One IP is one bucket, so exceeding
//     it buys 429s, not throughput. Queued work waits rather than piling on.
//   * Retries do NOT rotate IPs -- there is only one. A retry re-uses the same bucket, so
//     the retry policy is deliberately the same attempt cap, not the lane-avoidance dance.
//   * Streaming goes through the same client writer as a lane delta, so the SSE output is
//     byte-identical to the pooled path.
import { callUpstream } from './lane.js'
import { mergeToolDeltas } from './toolmerge.js'
import { config } from './config.js'

const enabled = () => String(config.DIRECT_MODE) === '1'
const MAX_CONCURRENT = Number(config.DIRECT_MAX_CONCURRENT || 4)

// Deltas for one direct call, batched on the same window a lane uses. One HTTP-free
// callback per token would put the whole token stream on the event loop for nothing.
const BATCH_MS = Number(config.DELTA_BATCH_MS || 40)

let inflight = 0
const waiters = []

const stats = () => ({ enabled: enabled(), inflight, waiting: waiters.length, max: MAX_CONCURRENT })

/** Take a concurrency slot, or wait for one. Never rejects: the dispatcher has its own deadline. */
async function acquire () {
  if (inflight < MAX_CONCURRENT) { inflight++; return true }
  await new Promise((resolve) => waiters.push(resolve))
  inflight++
  return true
}

function release () {
  inflight--
  const next = waiters.shift()
  if (next) next()
}

/**
 * Perform one upstream call directly and return the same result shape a lane would POST.
 *
 * `onDelta` / `onToolDelta` receive already-merged fragments, so the caller writes them
 * straight to the client exactly as it does for /lane/delta.
 */
export async function serveDirect (job, { onDelta, onToolCall, onToolDelta, onReasoning } = {}) {
  await acquire()
  let pendingText = []
  let pendingTools = []
  // Thinking is kept separate from text for the same reason as on a lane: it is
  // not the answer, and the caller measures delivery of the answer in characters.
  let pendingReasoning = []
  let timer = null
  let stopped = false

  const flush = () => {
    timer = null
    if (pendingReasoning.length) {
      const thought = pendingReasoning.join('')
      pendingReasoning = []
      onReasoning?.(thought)
    }
    if (pendingText.length) {
      const text = pendingText.join('')
      pendingText = []
      onDelta?.(text)
    }
    if (pendingTools.length) {
      const deltas = mergeToolDeltas(pendingTools, toolAcc)
      pendingTools = []
      onToolDelta?.(deltas)
    }
  }
  const schedule = () => {
    if (stopped || timer) return
    timer = setTimeout(flush, BATCH_MS)
    timer.unref?.()
  }
  // The tool accumulator outlives a flush, exactly as on a lane: identity and arguments
  // arrive in different batch windows, and folding per batch loses one of them.
  const toolAcc = new Map()

  const abort = (why) => { stopped = true; if (timer) { clearTimeout(timer); timer = null } }

  let result
  try {
    result = await callUpstream(job, {
      onDelta: job.stream ? (t) => { pendingText.push(t); schedule() } : undefined,
      onReasoning: job.stream ? (t) => { pendingReasoning.push(t); schedule() } : undefined,
      onToolCall: () => {
        // Withhold pre-tool text the client has not seen yet, so it is not shown
        // "I'll read that file" and then given nothing to run. Same rule as the lane.
        if (config.TOOL_MODE === 'self' || !job?.tools?.length) {
          pendingText = []
          if (timer && !pendingTools.length) { clearTimeout(timer); timer = null }
        }
        onToolCall?.()
      },
      onToolDelta: job.stream ? (d) => { pendingTools.push(...d); schedule() } : undefined,
    })
  } catch (e) {
    result = { kind: 'error', status: 0, ms: 0, data: null, raw: String(e?.message || e) }
  } finally {
    abort()
    release()
  }
  // Last flush: anything still batched is part of the answer.
  if (job.stream) {
    if (pendingReasoning.length) { onReasoning?.(pendingReasoning.join('')); pendingReasoning = [] }
    if (pendingText.length) { onDelta?.(pendingText.join('')); pendingText = [] }
    if (pendingTools.length) { onToolDelta?.(mergeToolDeltas(pendingTools, toolAcc)); pendingTools = [] }
  }
  return { ...result, streamedText: job.stream ? true : false, streamedTools: job.stream ? true : false }
}

export { enabled as directEnabled, stats as directStats }