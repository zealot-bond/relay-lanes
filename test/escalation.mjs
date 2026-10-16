// Escalation: a spent bucket on the relay's own IP must not become a dead relay.
//
// The whole point of the runner pool is a DIFFERENT egress IP, and therefore a different
// bucket. So when a direct call comes back rate-limited, the relay should start the pool
// and serve the request from a runner -- not hand the client a 429 and a paragraph of
// advice. These scenarios drive the real relay with a mock upstream that answers
// `FreeUsageLimitError` for direct calls and a real lane serving normally.
process.env.PORT = '18931'
process.env.HOST = '127.0.0.1'
process.env.RELAY_TOKEN = 't'
// Direct mode, escalation on, orchestrator pointed nowhere so nothing is dispatched.
process.env.DIRECT_MODE = '1'
process.env.DIRECT_ESCALATE = '1'
process.env.ESCALATE_WAIT_MS = '20000'
process.env.GH_REPO = 'no-such-owner/no-such-repo'
// The mocks use throwaway self-signed certs, so THIS process (which runs the relay) must
// trust them exactly as a lane does.
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'
process.env.LANES_ENABLED = '0'
process.env.ZEN_BASE = 'https://127.0.0.1:0'   // replaced below; direct calls must fail

const { execFileSync, spawn } = await import('node:child_process')
const fs = await import('node:fs')
const os = await import('node:os')
const path = await import('node:path')
const { fileURLToPath, pathToFileURL } = await import('node:url')

const here = path.dirname(fileURLToPath(import.meta.url))
const SRC = path.resolve(process.env.SRC_DIR || path.join(here, '..', 'src'))
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const H = { 'content-type': 'application/json', 'x-relay-token': 't' }  // matches process.env.RELAY_TOKEN
const kids = []
process.on('exit', () => kids.forEach((k) => { try { k.kill('SIGKILL') } catch { /* gone */ } }))

let failures = 0
const check = (name, cond, detail = '') => {
  if (cond) console.log(`  PASS  ${name}`)
  else { failures++; console.log(`  FAIL  ${name}${detail ? ' -- ' + detail : ''}`) }
}

// TWO mocks, in separate processes, because the claim under test is that a different egress
// IP has a different bucket -- so the relay and the lane must genuinely be talking to
// different upstreams:
//
//   spent  -- what this host's IP gets: every call answers FreeUsageLimitError
//   live   -- what a runner's IP gets: ordinary answers
const startMockProc = (scenario) => new Promise((resolve) => {
  const c = spawn(process.execPath, [path.join(here, 'mock-upstream.mjs')], {
    env: { ...process.env, MOCK_DEFAULT: scenario }, stdio: ['ignore', 'pipe', 'inherit'],
  })
  kids.push(c)
  let buf = ''
  const onData = (d) => {
    buf += d
    const m = /http:\/\/127\.0\.0\.1:(\d+)/.exec(buf)
    if (m) { c.stdout.off('data', onData); resolve(Number(m[1])) }
  }
  c.stdout.on('data', onData)
  setTimeout(() => resolve(0), 20000)
})

const spentPort = await startMockProc('free_usage_limit')
const livePort = await startMockProc('plain')
if (!spentPort || !livePort) { console.error('mocks failed to start'); process.exit(1) }
console.log(`mocks: spent=${spentPort} live=${livePort}`)

// Prove the "spent" upstream really does refuse, so a green run below means escalation
// worked rather than the mock being lenient.
{
  const probe = await fetch(`https://127.0.0.1:${spentPort}/v1/chat/completions`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'go' }] }),
  })
  const pj = await probe.json().catch(() => ({}))
  if (probe.status !== 429 || !/FreeUsageLimitError/.test(String(pj?.error?.type))) {
    console.error(`the spent mock is not refusing: status=${probe.status} body=${JSON.stringify(pj).slice(0, 120)}`)
    process.exit(1)
  }
  console.log('verified: the spent upstream answers 429 FreeUsageLimitError')
}

// The relay's own upstream: spent, always.
process.env.ZEN_BASE = `https://127.0.0.1:${spentPort}`

const S = await import(pathToFileURL(path.join(SRC, 'server.js')).href)
S.start()
await sleep(400)
const B = 'http://127.0.0.1:18931'
const post = (p, b) => fetch(B + p, { method: 'POST', headers: H, body: JSON.stringify(b) }).then((r) => r.json())

console.log('escalation: a spent bucket falls through to a runner IP\n')

// 1. With no lane anywhere, the client gets an honest 429 -- not a hang.
{
  const t = Date.now()
  const res = await fetch(B + '/v1/chat/completions', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }),
    signal: AbortSignal.timeout(60000),
  })
  const j = await res.json().catch(() => ({}))
  check('with no lane available the client gets 429, not a hang', res.status === 429, 'status ' + res.status + ' after ' + (Date.now() - t) + 'ms')
  check('and it is bounded by ESCALATE_WAIT_MS', Date.now() - t < 40000, (Date.now() - t) + 'ms')
  check('the escalation is armed and reported on /health',
    (await (await fetch(B + '/health')).json()).escalate?.armed === true)
}

// 2. Register a lane that points at the mock. The next request must be served by it.
{
  const env = { ...process.env, RELAY_URL: B, RELAY_TOKEN: 't', LANE_ID: 'esc-1', ZEN_BASE: `https://127.0.0.1:${livePort}`, NODE_TLS_REJECT_UNAUTHORIZED: '0' }
  const lane = spawn(process.execPath, [path.join(SRC, 'worker.js')], { env, stdio: 'ignore' })
  kids.push(lane)
  const up = await (async () => {
    for (let i = 0; i < 120; i++) {
      const h = await (await fetch(B + '/health')).json().catch(() => null)
      if (h && h.lanes >= 1) return h
      await sleep(250)
    }
    return null
  })()
  check('a lane registers', Boolean(up && up.lanes >= 1), JSON.stringify(up && { lanes: up.lanes }))

  // NO [[S:...]] marker: the mock dispatches on a marker when one is present, and that
  // would override MOCK_DEFAULT and make the "spent" upstream answer normally. The first
  // version of this test passed for exactly that reason -- the direct call succeeded
  // against the mock that was supposed to be refusing, so escalation was never exercised.
  const res = await fetch(B + '/v1/chat/completions', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'go' }] }),
    signal: AbortSignal.timeout(60000),
  })
  const j = await res.json().catch(() => ({}))
  const text = j?.choices?.[0]?.message?.content
  check('the exhausted direct IP no longer fails the request', res.status === 200, 'status ' + res.status + ' ' + String(j?.error?.message).slice(0, 60))
  check('it is served by the runner', text === 'Hello world', JSON.stringify(text))
}

// 3. And the lane path is used for subsequent traffic too, without re-waiting each time.
{
  const t = Date.now()
  const res = await fetch(B + '/v1/chat/completions', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'go' }] }),
    signal: AbortSignal.timeout(60000),
  })
  await res.json().catch(() => ({}))
  check('the next request is served immediately by the lane', res.status === 200 && Date.now() - t < 30000,
    res.status + ' in ' + (Date.now() - t) + 'ms')
}

// 4. "Why is it using direct mode first?" -- every request was paying for a doomed direct
// call before escalating. Measured on the panel: 782ms, 710ms, 742ms, 1010ms, 1544ms of
// pure latency on calls that were certain to 429. The direct path must switch OFF.
{
  const esc = (await (await fetch(B + '/health')).json()).escalate
  check('the direct path is reported blocked', esc?.directBlocked === true, JSON.stringify(esc))
  check('with a visible remaining window', esc?.directBlockedForS > 0, JSON.stringify(esc))

  let touches = 0
  const realFetch = globalThis.fetch
  globalThis.fetch = (u, o) => { if (String(u).includes(String(spentPort))) touches++; return realFetch(u, o) }
  const rs = await Promise.all([...Array(6)].map(() => fetch(B + '/v1/chat/completions', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'go' }] }),
    signal: AbortSignal.timeout(60000),
  })))
  globalThis.fetch = realFetch
  const bodies = await Promise.all(rs.map((r) => r.json().catch(() => ({}))))
  check('six more requests are all served by the runner',
    bodies.every((b) => b?.choices?.[0]?.message?.content === 'Hello world'),
    JSON.stringify(bodies.map((b) => b?.choices?.[0]?.message?.content || String(b?.error?.message).slice(0, 34))))
  check('and NONE of them retried the known-spent bucket', touches === 0, touches + ' wasted direct call(s)')
}

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL ESCALATION CHECKS PASSED')
process.exit(failures ? 1 : 0)