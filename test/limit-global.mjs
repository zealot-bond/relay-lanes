// A daily free-tier window is not a lane fault.
//
// The upstream answers 429 with `Retry-After: 7200`. Every egress IP would say the same,
// so the relay must keep the lane (replacing it buys another copy of the same answer),
// report the provider's own reset to the client, and make no further upstream call while
// the breaker is armed. Before this, each such 429 retired a runner and the next request
// retired another -- one request per lane for a window that was closed for hours.
//
//   node test/limit-global.mjs
import { spawn } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { start as startMock, stop as stopMock, seen } from './mock-upstream.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
const ROOT = path.join(here, '..')
const PORT = Number(process.env.PORT || 26033)
const BASE = `http://127.0.0.1:${PORT}`
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

const health = () => fetch(BASE + '/health', { signal: AbortSignal.timeout(3000) }).then((r) => r.json())

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

const mockPort = await startMock()
// Only the chat path matters here: the catalogue refresh also hits the mock (/v1/models)
// and would make an "upstream was not called" assertion pass or fail by timing.
const chatCalls = () => seen.filter((e) => /chat\/completions/.test(e.path)).length
const env = {
  ...process.env,
  NODE_TLS_REJECT_UNAUTHORIZED: '0',
  PORT: String(PORT),
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

try {
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
  if (!await waitFor(async () => (await health()).ok)) {
    console.error('relay did not start')
    process.exit(1)
  }
  kids.push(spawn(process.execPath, [path.join(ROOT, 'src/worker.js')], {
    env: { ...env, RELAY_URL: BASE, LANE_ID: 't-1' },
    stdio: 'ignore',
  }))
  if (!await waitFor(async () => (await health()).lanes >= 1)) {
    console.error('lane did not register')
    process.exit(1)
  }

  console.log('1. the client is told the provider\'s own reset window')
  const first = await ask()
  check('the request fails with 429', first.status === 429, String(first.status))
  check('Retry-After is the upstream\'s 7200, not a guessed 900', first.retryAfter === '7200', String(first.retryAfter))
  check('the message names when the window reopens', /reopens at \d\d:\d\d UTC/.test(first.err || ''), String(first.err).slice(0, 120))

  console.log('2. the lane is kept, because its IP was not the variable')
  const afterFirst = await health()
  check('the lane is still registered', afterFirst.lanes >= 1, String(afterFirst.lanes))
  check('no lane was retired', afterFirst.retired === 0, String(afterFirst.retired))
  check('the breaker is armed with the upstream window',
    afterFirst.exhaustion?.active === true && afterFirst.exhaustion?.retryAfterS === 7200,
    JSON.stringify(afterFirst.exhaustion))

  console.log('3. no upstream call is made while the window is closed')
  const callsBefore = chatCalls()
  const second = await ask()
  const callsAfter = chatCalls()
  check('the second request still answers 429 with the same window',
    second.status === 429 && second.retryAfter === '7200', `${second.status} ${second.retryAfter}`)
  check('it never reached the upstream', callsAfter === callsBefore, `${callsBefore} -> ${callsAfter}`)
  check('and it answers immediately instead of taking a lane round trip', second.ms < 1500, `${second.ms}ms`)
  const afterSecond = await health()
  check('the lane survived a second request too', afterSecond.lanes >= 1 && afterSecond.retired === 0,
    `lanes=${afterSecond.lanes} retired=${afterSecond.retired}`)

  console.log('4. a streaming harness is told the same thing')
  const stream = await askStream()
  check('the stream is accepted, because the response has already started', stream.status === 200, String(stream.status))
  check('the error event names the reopen time', /reopens at \d\d:\d\d UTC/.test(stream.err || ''), String(stream.err).slice(0, 160))
  check('and does not blame DIRECT_MODE when lanes are primary', !/DIRECT_MODE/.test(stream.err || ''))
  check('the stream made no upstream call either', chatCalls() === callsAfter, `${callsAfter} -> ${chatCalls()}`)
} finally {
  killKids()
  stopMock()
}

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL GLOBAL-EXHAUSTION CHECKS PASSED')
process.exit(failures ? 1 : 0)
