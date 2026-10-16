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

// Circuit breaker for the direct path.
//
// Escalation alone is not enough, because "direct first" costs a wasted upstream call on
// EVERY request for as long as the bucket is spent. Measured on the panel: five
// consecutive requests burned 782ms, 710ms, 742ms, 1010ms and 1544ms on direct calls that
// were certain to 429 before escalating to a runner. That is up to 1.5s of pure latency
// added to every request, forever, to serve a bucket already known to be empty.
//
// So the direct path switches OFF on the first `FreeUsageLimitError` and stays off for
// DIRECT_RECOVER_MS. Requests go straight to lanes. After the window, ONE probe is allowed
// through (half-open), because the quota resets and the single IP becomes free again --
// which is the whole saving that direct mode exists for.
let blockedUntil = 0
let consecutiveLimited = 0
let probes = 0

const recoverMs = () => Number(config.DIRECT_RECOVER_MS || 600000)

/** Is the direct path worth trying right now? */
export const directAvailable = () => Date.now() >= blockedUntil

export const directBlocked = () => Date.now() < blockedUntil

export function noteDirectLimited () {
  consecutiveLimited++
  // A FreeUsageLimitError is unambiguous -- unlike a timeout or a 5xx -- so one is enough.
  blockedUntil = Date.now() + recoverMs()
  probes = 0
}

export function noteDirectOk () {
  consecutiveLimited = 0
  blockedUntil = 0
}

/** Half-open: a window that has expired allows a probe, then goes back to blocking. */
export function noteDirectProbed () {
  if (probes > 0) return true
  probes = 1
  blockedUntil = Date.now() + recoverMs()
  return false
}

/** main.js supplies this so the orchestrator can be started lazily, on first exhaustion. */
export function onEscalate (fn) { armHook = fn }

export const stats = () => ({
  enabled: enabled(),
  armed,
  waitedMs: waitMs(),
  directBlocked: directBlocked(),
  directBlockedForS: directBlocked() ? Math.ceil((blockedUntil - Date.now()) / 1000) : 0,
  consecutiveLimited,
  recoverMs: recoverMs(),
})

/** Test hook: forget the breaker ever tripped. */
export function resetBreaker () {
  blockedUntil = 0
  consecutiveLimited = 0
  probes = 0
}

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