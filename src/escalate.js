// Escalation: direct mode first, the runner pool when this IP's bucket is spent.
//
// DIRECT_MODE answers from the relay's own egress IP. That is the fastest path and the
// simplest, but the free tier's quota is per IP, so the moment that bucket is spent EVERY
// model goes quiet at once and there is nothing to retry onto. This makes the pool the
// answer instead of a piece of advice in an error message:
//
//   direct call
//     -> ok            : done
//     -> limited       : arm the escalation (the orchestrator starts if it is not already
//                        running), wait briefly for runners to appear, then serve from a
//                        runner IP
//     -> anything else : report it
//
// So one machine serves light traffic with no CI spend at all, and sustained traffic grows
// a lane pool only when it is actually needed -- which is the thing that was missing.
//
// Bounded on purpose: if no lane appears within ESCALATE_WAIT_MS the client still gets its
// 429 rather than hanging. A runner pool takes a minute or two to come up, so holding a
// request for longer than that would be worse than telling the truth.
import { config } from './config.js'

const enabled = () => String(config.DIRECT_ESCALATE) === '1'
const waitMs = () => Number(config.ESCALATE_WAIT_MS || 90000)

let armed = false
let armHook = null

/** main.js supplies this so the orchestrator can be started lazily, on first exhaustion. */
export function onEscalate (fn) { armHook = fn }

export const stats = () => ({ enabled: enabled(), armed, waitedMs: waitMs() })

/**
 * Called when a direct call comes back `limited`.
 *
 * @param hasLane  () => boolean   is a usable lane registered right now?
 * @returns {Promise<boolean>}     true if a lane is available to serve the request
 */
export async function escalate (hasLane) {
  if (!enabled()) return false
  if (!armed) {
    armed = true
    console.error('[relay] direct-mode bucket exhausted -- escalating to the runner pool. ' +
      'Waiting for lanes; the orchestrator will start them if it is not already running.')
    try { armHook?.() } catch (e) { console.error('[relay] could not arm escalation:', e?.message) }
  }
  // A lane may already be there (from a previous escalation, or a lane mode run).
  if (hasLane()) return true

  const until = Date.now() + waitMs()
  while (Date.now() < until) {
    await new Promise((r) => setTimeout(r, 1000))
    if (hasLane()) {
      console.log('[relay] a lane is available -- continuing on a runner IP')
      return true
    }
  }
  console.error(`[relay] no lane appeared within ${waitMs()}ms; reporting the rate limit`)
  return false
}

/** Test hook: forget that escalation was ever armed. */
export function resetEscalation () { armed = false }