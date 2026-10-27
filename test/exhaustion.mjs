// The global-exhaustion breaker is DISABLED by default (EXHAUSTION_GLOBAL_S=0).
//
// Its premise was that a long Retry-After means every egress IP sees the same closed
// window. Measured 2026-10-26 by bisection, that long Retry-After is the free tier's
// GATE: a request not shaped like the first-party client (Bun's TLS fingerprint, an
// <env> block with a working directory, the four gateway tool names) gets a fake
// `429 FreeUsageLimitError` with a midnight-UTC reset, on any IP, while a real client on
// the same IP answers 200. So the gate is prevented (src/egress.js, ensureEnvBlock()),
// and the breaker is off unless an operator has measured a genuinely global window.
//
// These checks pin BOTH halves: threshold 0 means nothing is ever global, and an explicit
// threshold still classifies as before for a deployment that wants it.
//
//   node test/exhaustion.mjs
import * as exhaustion from '../src/exhaustion.js'
import { config } from '../src/config.js'

let failures = 0
const check = (name, ok, detail = '') => {
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' -- ' + detail : ''}`)
  if (!ok) failures++
}

exhaustion.reset()

console.log('1. the breaker is off unless a deployment turns it on')
check('the default threshold disables it', config.EXHAUSTION_GLOBAL_S === 0)
check('so not even a daily reset is global',
  exhaustion.isGlobal({ kind: 'limited', retryAfter: 37551 }) === false)
check('a zero threshold is disabled even when passed explicitly',
  exhaustion.isGlobal({ kind: 'limited', retryAfter: 37551 }, 0) === false)
check('a negative threshold is disabled too',
  exhaustion.isGlobal({ kind: 'limited', retryAfter: 37551 }, -1) === false)

console.log('2. an explicitly configured threshold still classifies')
check('below it the lane is still replaced',
  exhaustion.isGlobal({ kind: 'limited', retryAfter: 1799 }, 1800) === false)
check('at it the window is global',
  exhaustion.isGlobal({ kind: 'limited', retryAfter: 1800 }, 1800) === true)
check('a short window is per-IP even when configured',
  exhaustion.isGlobal({ kind: 'limited', retryAfter: 60 }, 1800) === false)
check('a limit with no Retry-After cannot be judged, so the old behaviour stays',
  exhaustion.isGlobal({ kind: 'limited' }, 1800) === false)
check('a blacklisted egress is per-IP by definition',
  exhaustion.isGlobal({ kind: 'ip_blocked', retryAfter: 37551 }, 1800) === false)
check('a provider error carrying Retry-After is not a spent bucket',
  exhaustion.isGlobal({ kind: 'provider_error', retryAfter: 37551 }, 1800) === false)

console.log('3. arming it (explicit, as a deployment would)')
check('it starts inactive', exhaustion.active() === false && exhaustion.status().until === null)
const held = exhaustion.note(7200, 'alpha-001')
const st = exhaustion.status()
check('the wait is the provider\'s own', held === 7200 && st.retryAfterS === 7200)
check('it is active', st.active === true)
check('it names the model and the reopen time', st.model === 'alpha-001' && typeof st.until === 'string')
check('the reset is about two hours away',
  Math.abs(new Date(st.until).getTime() - Date.now() - 7200 * 1000) < 5000, st.until)
check('a second reset extends it and counts', (() => {
  exhaustion.note(7300, 'alpha-002')
  const s2 = exhaustion.status()
  return s2.hits === 2 && s2.retryAfterS === 7300 && s2.model === 'alpha-002'
})())

console.log('4. the answer a client gets without touching a lane')
const r = exhaustion.result('alpha-003')
check('it is a 429 carrying the provider\'s window',
  r.kind === 'limited' && r.status === 429 && r.retryAfter === 7300)
check('it explains the reset in its own words', /reopens at/.test(r.raw))
check('it claims no lane', r.laneId === null)

console.log('5. it expires on its own, so a reset needs no restart')
exhaustion.note(1, 'alpha-003')
check('still active inside the window', exhaustion.active() === true)
await new Promise((resolve) => setTimeout(resolve, 1200))
check('inactive once the window passes', exhaustion.active() === false && exhaustion.status().active === false)

exhaustion.reset()
check('reset clears it for the next scenario', exhaustion.active() === false && exhaustion.status().hits === 0)

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL EXHAUSTION CHECKS PASSED')
process.exit(failures ? 1 : 0)
