// A long Retry-After is GLOBAL exhaustion: every egress IP sees the same closed window,
// so the lane must not be retired and no upstream call should be made until it reopens.
// A short one is still per-IP, where retiring the lane and retrying elsewhere is right.
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

console.log('1. what counts as a global window')
check('a daily reset is global', exhaustion.isGlobal({ kind: 'limited', retryAfter: 37551 }))
check('a short window is per-IP, so the lane is still replaced',
  exhaustion.isGlobal({ kind: 'limited', retryAfter: 60 }) === false)
check('a limit with no Retry-After cannot be judged, so the old behaviour stays',
  exhaustion.isGlobal({ kind: 'limited' }) === false)
check('a blacklisted egress is per-IP by definition',
  exhaustion.isGlobal({ kind: 'ip_blocked', retryAfter: 37551 }) === false)
check('a provider error carrying Retry-After is not a spent bucket',
  exhaustion.isGlobal({ kind: 'provider_error', retryAfter: 37551 }) === false)
check('the threshold is deployment config', exhaustion.isGlobal({ kind: 'limited', retryAfter: 1799 }) === false &&
  exhaustion.isGlobal({ kind: 'limited', retryAfter: 1800 }) === true &&
  config.EXHAUSTION_GLOBAL_S === 1800)

console.log('2. arming it')
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

console.log('3. the answer a client gets without touching a lane')
const r = exhaustion.result('alpha-003')
check('it is a 429 carrying the provider\'s window',
  r.kind === 'limited' && r.status === 429 && r.retryAfter === 7300)
check('it explains the reset in its own words', /reopens at/.test(r.raw))
check('it claims no lane', r.laneId === null)

console.log('4. it expires on its own, so a reset needs no restart')
exhaustion.note(1, 'alpha-003')
check('still active inside the window', exhaustion.active() === true)
await new Promise((resolve) => setTimeout(resolve, 1200))
check('inactive once the window passes', exhaustion.active() === false && exhaustion.status().active === false)

exhaustion.reset()
check('reset clears it for the next scenario', exhaustion.active() === false && exhaustion.status().hits === 0)

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL EXHAUSTION CHECKS PASSED')
process.exit(failures ? 1 : 0)
