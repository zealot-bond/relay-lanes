// What a long `Retry-After` must do, in BOTH configurations.
//
// Background: on 2026-10-26 the upstream answered every request with
// `429 FreeUsageLimitError, Retry-After: 37551` (the reset at 00:00 UTC) from this host
// and from every runner at once, which was read as one window shared by all egress IPs.
// That reading was wrong. By bisection the same day, the long 429 is the free tier's
// GATE: it is returned to any request not shaped like the first-party client (Bun's TLS
// fingerprint, an `<env>` block with a working directory, the four gateway tool names),
// on any IP, while a real client on the same IP answers 200. The gate is now prevented
// (src/egress.js, ensureEnvBlock()), so the breaker is OFF by default.
//
// Both halves are pinned here, because each one alone can look right:
//   1. DEFAULT (EXHAUSTION_GLOBAL_S=0): a long Retry-After is a per-IP limit like any
//      other -- the lane is retired and the client gets the 429. The relay must NOT close
//      itself on the gate's fake quota (that is what made a fixable bug look like an
//      exhausted account for two days).
//   2. OPT-IN (EXHAUSTION_GLOBAL_S>0): a measured global window keeps the lane, names the
//      provider's reset, and makes no further upstream call until it passes.
//
//   node test/limit-global.mjs
import { spawn } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { start as startMock, stop as stopMock, seen } from './mock-upstream.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
const ROOT = path.join(here, '..')
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const kids = []
const killKids = () => { for (const k of kids) { try { k.kill('SIGKILL') } catch { /* gone */ } } }

let failures = 0
const check = (name, ok, detail = '') => {
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' -- ' + detail : ''}`)
  if (!ok) failures++
}

const waitFor = async (fn, ms = 30000) => {
  const until = Date.now() + ms
  while (Date.now() < until) {
    try { if (await fn()) return true } catch { /* not up yet */ }
    await sleep(200)
  }
  return false
}

const mockPort = await startMock()
// Only the chat path matters here: the catalogue refresh also hits the mock (/models)
// and would make an "upstream was not called" assertion pass or fail by timing.
const chatCalls = () => seen.filter((e) => /chat\/completions/.test(e.path)).length

const baseEnv = {
  ...process.env,
  NODE_TLS_REJECT_UNAUTHORIZED: '0',
  HOST: '127.0.0.1',
  RELAY_TOKEN: 't',
  // Lanes-only, and no real GitHub dispatch: this test must never spend CI minutes.
  DIRECT_MODE: '0',
  DIRECT_FALLBACK: '0',
  LANES_ENABLED: '0',
  ZEN_BASE: `https://127.0.0.1:${mockPort}`,
  CATALOGUE_ALLOW: '*',
  MODELS_CATALOGUE_URL: 'off',
}

/** Boot one relay + one lane on `port`. Returns the health/ask helpers for it. */
async function boot (port, laneId, extra = {}) {
  const BASE = `http://127.0.0.1:${port}`
  const env = { ...baseEnv, PORT: String(port), ...extra }

  // Refuse to run against a squatter: its lanes would register against the wrong relay
  // and the failure would look like an upstream fault.
  try {
    const probe = await fetch(BASE + '/health', { signal: AbortSignal.timeout(1500) })
    if (probe.ok) {
      console.error(`refusing to start: something is already serving ${BASE}. Pick another PORT.`)
      process.exit(1)
    }
  } catch { /* nothing there, which is what we want */ }

  kids.push(spawn(process.execPath, [path.join(ROOT, 'src/main.js')], { env, stdio: 'ignore' }))
  const health = () => fetch(BASE + '/health', { signal: AbortSignal.timeout(3000) }).then((r) => r.json())
  if (!await waitFor(async () => (await health()).ok)) {
    console.error(`relay did not start on ${port}`)
    process.exit(1)
  }
  kids.push(spawn(process.execPath, [path.join(ROOT, 'src/worker.js')], {
    env: { ...env, RELAY_URL: BASE, LANE_ID: laneId },
    stdio: 'ignore',
  }))
  if (!await waitFor(async () => (await health()).lanes >= 1)) {
    console.error(`lane did not register on ${port}`)
    process.exit(1)
  }

  const request = (stream) => fetch(BASE + '/v1/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'github/big-pickle',
      stream,
      messages: [{ role: 'user', content: '[[S:free_usage_limit_daily]] go' }],
    }),
    signal: AbortSignal.timeout(60000),
  })

  const ask = async () => {
    const t0 = Date.now()
    const res = await request(false)
    const body = await res.text()
    let err = null
    try { err = JSON.parse(body)?.error?.message || null } catch { /* not json */ }
    return { status: res.status, retryAfter: res.headers.get('retry-after'), ms: Date.now() - t0, err }
  }

  // A harness streams, so the truth has to arrive as an event: the status is already 200.
  const askStream = async () => {
    const res = await request(true)
    const body = await res.text()
    let err = null
    for (const line of body.split('\n')) {
      if (!line.startsWith('data: ')) continue
      try { err = JSON.parse(line.slice(6))?.error?.message || err } catch { /* not json */ }
    }
    return { status: res.status, retryAfter: res.headers.get('retry-after'), err }
  }

  return { BASE, health, ask, askStream }
}

try {
  console.log('1. DEFAULT: the gate\'s fake window must not close the relay')
  {
    // MAX_HOLD_MS bounds the wait for a lane after the only lane was retired; without a
    // short value this scenario would hold for the production ten minutes.
    const relay = await boot(26033, 't-default', { MAX_HOLD_MS: '3000' })
    check('the breaker starts unarmed',
      (await relay.health()).exhaustion?.active === false, JSON.stringify((await relay.health()).exhaustion))
    const callsBefore = chatCalls()
    const first = await relay.ask()
    const h = await relay.health()
    check('the client gets the provider\'s 429', first.status === 429, String(first.status))
    check('carrying the upstream\'s own Retry-After', first.retryAfter === '7200', String(first.retryAfter))
    check('the breaker is still NOT armed', h.exhaustion?.active === false, JSON.stringify(h.exhaustion))
    check('it is disabled by config, not by luck', h.exhaustion?.thresholdS === 0, String(h.exhaustion?.thresholdS))
    check('the lane WAS retired, like any per-IP limit', h.retired >= 1, String(h.retired))
    check('so the request really reached the upstream', chatCalls() > callsBefore,
      `${callsBefore} -> ${chatCalls()}`)
    check('and it did not hold the client for the full ten minutes', first.ms < 20000, `${first.ms}ms`)
    check('the message does not promise a global reset it cannot know', !/Every egress IP sees/.test(first.err || ''))
  }

  console.log('2. OPT-IN: a measured global window is honoured')
  {
    const relay = await boot(26034, 't-global', { EXHAUSTION_GLOBAL_S: '7200' })
    const callsBefore = chatCalls()
    const first = await relay.ask()
    check('the request fails with 429', first.status === 429, String(first.status))
    check('Retry-After is the upstream\'s 7200, not a guessed 900', first.retryAfter === '7200', String(first.retryAfter))
    check('the message names when the window reopens', /reopens at \d\d:\d\d UTC/.test(first.err || ''), String(first.err).slice(0, 120))

    const afterFirst = await relay.health()
    check('the lane is kept, because its IP was not the variable', afterFirst.lanes >= 1, String(afterFirst.lanes))
    check('no lane was retired', afterFirst.retired === 0, String(afterFirst.retired))
    check('the breaker is armed with the upstream window',
      afterFirst.exhaustion?.active === true && afterFirst.exhaustion?.retryAfterS === 7200,
      JSON.stringify(afterFirst.exhaustion))

    const callsAfterFirst = chatCalls()
    const second = await relay.ask()
    check('the second request still answers 429 with the same window',
      second.status === 429 && second.retryAfter === '7200', `${second.status} ${second.retryAfter}`)
    check('it never reached the upstream', chatCalls() === callsAfterFirst, `${callsAfterFirst} -> ${chatCalls()}`)
    check('and it answers immediately instead of taking a lane round trip', second.ms < 1500, `${second.ms}ms`)
    const afterSecond = await relay.health()
    check('the lane survived a second request too', afterSecond.lanes >= 1 && afterSecond.retired === 0,
      `lanes=${afterSecond.lanes} retired=${afterSecond.retired}`)

    const stream = await relay.askStream()
    check('the stream is accepted, because the response has already started', stream.status === 200, String(stream.status))
    check('the error event names the reopen time', /reopens at \d\d:\d\d UTC/.test(stream.err || ''), String(stream.err).slice(0, 160))
    check('and does not blame DIRECT_MODE when lanes are primary', !/DIRECT_MODE/.test(stream.err || ''))
    check('the stream made no upstream call either', chatCalls() === callsAfterFirst, `${callsAfterFirst} -> ${chatCalls()}`)
  }
} finally {
  killKids()
  stopMock()
}

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL LIMIT-WINDOW CHECKS PASSED')
process.exit(failures ? 1 : 0)
