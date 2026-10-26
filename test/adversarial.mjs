// End-to-end tests against the REAL relay and the REAL lanes, with only the provider
// replaced by a local mock (test/mock-upstream.mjs).
//
// Why this exists: the other suites test pieces. This one boots src/main.js and
// src/worker.js unmodified and asserts what a CLIENT receives, which is the only place
// a mismatch between the streamed index space and the folded completion can show up.
//
//   node test/adversarial.mjs
//   SRC_DIR=/path/to/older/src node test/adversarial.mjs     # negative control
//   NO_BOOT=1 PORT=26002 node test/adversarial.mjs            # test a RUNNING relay
//
// NO_BOOT targets an already-running relay instead of starting one, which is how the
// packed jar is verified. Note the trap it avoids: pointing this script at a port that
// another relay already owns does NOT fail loudly -- its own relay cannot bind, but
// the lanes it spawns register against whoever answers on that port. The run then
// looks like a mass upstream failure rather than a port clash.
//
// The negative control is not optional. Two of the bugs below passed every other
// suite, because each was green in isolation and only wrong in composition.
import { spawn, execFileSync } from 'node:child_process'
import fs from 'node:fs'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { start as startMock, stop as stopMock } from './mock-upstream.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
const SRC = path.resolve(process.env.SRC_DIR || path.join(here, '..', 'src'))
const LANES = Number(process.env.LANES || 6)
const PORT = Number(process.env.PORT || 26021)
const BASE = `http://127.0.0.1:${PORT}`
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const NO_BOOT_EARLY = process.env.NO_BOOT === '1'
const kids = []

const killKids = () => {
  for (const k of kids) { try { k.kill('SIGKILL') } catch { /* gone */ } }
  kids.length = 0
}
process.on('exit', () => { killKids(); if (mockPort) stopMock() })

// ---------------------------------------------------------------- boot
const waitFor = async (check, ms = 25000) => {
  const until = Date.now() + ms
  while (Date.now() < until) {
    try { if (await check()) return true } catch { /* not up yet */ }
    await sleep(250)
  }
  return false
}

// In NO_BOOT mode the relay (and its lanes) already exist, so no mock is started here:
// it would be a second, unused upstream.
const mockPort = NO_BOOT_EARLY ? 0 : await startMock()
const ZEN = mockPort ? `https://127.0.0.1:${mockPort}` : ''
const env = {
  ...process.env,
  NODE_TLS_REJECT_UNAUTHORIZED: '0',
  // Deployment catalogue policy: the scenarios use mock ids, and no test may fetch the
  // real published catalogue.
  CATALOGUE_ALLOW: '*',
  MODELS_CATALOGUE_URL: 'off',
}
const spawnNode = (script, extra) => {
  const k = spawn(process.execPath, [path.join(SRC, script)], { env: { ...env, ...extra }, stdio: 'ignore' })
  kids.push(k)
  return k
}

const NO_BOOT = process.env.NO_BOOT === '1'
if (NO_BOOT) {
  const h = await (await fetch(BASE + '/health', { signal: AbortSignal.timeout(8000) })).json()
  console.log(`testing a RUNNING relay: ${BASE}`)
  console.log(`  build ${h.build} | lanes ${h.lanes} | laneCode ${JSON.stringify(h.laneCode)}\n`)
  console.log('NOTE: its lanes must already point at a mock upstream for these checks to pass.\n')
} else {
  console.log(`source under test: ${SRC}\nbooting relay + ${LANES} real lanes against a mock upstream...`)
  // Refuse early if something already owns the port: otherwise the relay below cannot
  // bind and the lanes register against the squatter.
  try {
    const probe = await fetch(BASE + '/health', { signal: AbortSignal.timeout(2000) })
    if (probe.ok) {
      console.error(`refusing to start: something is already serving ${BASE}.`)
      console.error('Use NO_BOOT=1 to test it deliberately, or pick another PORT.')
      process.exit(1)
    }
  } catch { /* nothing there, which is what we want */ }
  // These scenarios are about the LANE path, and lanes are primary again, so pin direct
  // off explicitly rather than relying on a default.
  spawnNode('main.js', { PORT: String(PORT), HOST: '127.0.0.1', RELAY_TOKEN: 't', DIRECT_MODE: '0', DIRECT_FALLBACK: '0' })
  if (!await waitFor(async () => (await fetch(BASE + '/health', { signal: AbortSignal.timeout(2000) })).ok)) {
    console.error('relay did not start'); process.exit(1)
  }
  for (let n = 1; n <= LANES; n++) {
    spawnNode('worker.js', { RELAY_URL: BASE, RELAY_TOKEN: 't', LANE_ID: `t-${n}`, ZEN_BASE: ZEN })
  }
  if (!await waitFor(async () => (await (await fetch(BASE + '/health')).json()).lanes >= LANES)) {
    console.error('lanes did not register'); process.exit(1)
  }
  console.log('up\n')
}

// ---------------------------------------------------------------- client
const BASH = { type: 'object', properties: { command: { type: 'string' }, description: { type: 'string' } }, required: ['command', 'description'] }
const TOOLS = [{ type: 'function', function: { name: 'bash', description: 'Run a bash command.', parameters: BASH } }]

async function ask ({ scenario, tools, stream = true, messages, timeout = 60000, model = 'big-pickle', base }) {
  const t0 = Date.now()
  const body = { model: `github/${model}`, stream, messages: messages || [{ role: 'user', content: `[[S:${scenario}]] go` }] }
  if (tools) body.tools = tools
  const res = await fetch((base || BASE) + '/v1/chat/completions', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body), signal: AbortSignal.timeout(timeout),
  })
  const status = res.status
  if (!stream) {
    const txt = await res.text()
    let j = null; try { j = JSON.parse(txt) } catch { /* raw */ }
    const m = j?.choices?.[0]?.message
    return { status, ms: Date.now() - t0, text: String(m?.content || ''),
      calls: (m?.tool_calls || []).map((c) => ({ name: c.function?.name, args: c.function?.arguments })),
      err: j?.error ? String(j.error.message).slice(0, 240) : null }
  }
  // Read as it arrives: timing a fully-buffered read would report every frame at the
  // moment the stream ended.
  const reader = res.body.getReader()
  const dec = new TextDecoder()
  let buf = '', raw = '', text = '', err = null, finish = null, ttft = null
  const acc = new Map()
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    const s = dec.decode(value, { stream: true })
    raw += s; buf += s
    let k
    while ((k = buf.indexOf('\n\n')) !== -1) {
      const frame = buf.slice(0, k); buf = buf.slice(k + 2)
      const line = frame.split('\n').find((l) => l.startsWith('data:'))
      if (!line) continue
      const p = line.slice(5).trim()
      if (!p || p === '[DONE]') continue
      let e; try { e = JSON.parse(p) } catch { continue }
      if (e.error) { err = String(e.error.message).slice(0, 240); continue }
      const ch = e.choices?.[0]
      if (ch?.finish_reason) finish = ch.finish_reason
      const d = ch?.delta || {}
      if (typeof d.content === 'string' && d.content) { if (ttft === null) ttft = Date.now() - t0; text += d.content }
      for (const tc of d.tool_calls || []) {
        const i = tc.index ?? 0
        const c = acc.get(i) || { name: '', args: '', id: '' }
        if (tc.id) c.id = tc.id
        if (tc.function?.name) c.name += tc.function.name
        if (tc.function?.arguments) c.args += tc.function.arguments
        acc.set(i, c)
      }
    }
  }
  return { status, ms: Date.now() - t0, ttft, text, calls: [...acc.values()], finish, err,
    raw: raw.slice(0, 300), sawDone: raw.includes('[DONE]') }
}

let pass = 0, fail = 0
const check = (name, cond, detail = '') => {
  if (cond) { pass++; console.log(`  PASS  ${name}`) } else { fail++; console.log(`  FAIL  ${name}${detail ? ' -- ' + detail : ''}`) }
}
const parse = (s) => { try { return JSON.parse(s) } catch { return null } }
const group = (n) => console.log(`\n${n}`)

// ---------------------------------------------------------------- streamed text
group('streamed text reassembles exactly')
{
  const r = await ask({ scenario: 'plain' })
  check('200', r.status === 200, 'status ' + r.status)
  check('content is exactly "Hello world"', r.text === 'Hello world', JSON.stringify(r.text))
  check('finish_reason stop', r.finish === 'stop', String(r.finish))
  check('stream ends with [DONE]', r.sawDone)
  check('no error frame', !r.err, String(r.err))
}
check('a stream delivered in a single write still parses',
  (await ask({ scenario: 'one_shot' })).text === 'all at once, no newlines between')
check('CRLF line endings are accepted',
  (await ask({ scenario: 'crlf' })).text === 'crlf works')
check('multi-byte UTF-8 split one byte per write is intact',
  (await ask({ scenario: 'utf8_split' })).text === '日本語 😀 café — ✓')
check('a comment and a non-JSON data line do not eat the stream',
  (await ask({ scenario: 'keepalive_then_data' })).text === 'survived the noise')
check('reasoning deltas are not shown as content',
  (await ask({ scenario: 'reasoning_then_text' })).text === 'The answer is 4. That is all.')
{
  const r = await ask({ scenario: 'json_not_sse', tools: TOOLS })
  check('a plain JSON body on a stream request is still usable',
    r.status === 200 && (r.text === 'json body' || r.calls.length > 0),
    JSON.stringify({ t: r.text, calls: r.calls.length, err: r.err }))
}

// ---------------------------------------------------------------- tool calls
group('tool calls reassemble exactly')
{
  const r = await ask({ scenario: 'tool_small_deltas', tools: TOOLS })
  const want = '{"command":"ls -la /tmp","description":"List files in /tmp"}'
  check('200', r.status === 200, 'status ' + r.status)
  check('exactly one call', r.calls.length === 1, JSON.stringify(r.calls.length))
  check('name is exactly "bash", not doubled', r.calls[0]?.name === 'bash', JSON.stringify(r.calls[0]?.name))
  check('arguments are byte-exact', r.calls[0]?.args === want, JSON.stringify(r.calls[0]?.args))
  check('both required properties are present', ['command', 'description'].every((k) => parse(r.calls[0]?.args)?.[k]))
  check('finish_reason is tool_calls', r.finish === 'tool_calls', String(r.finish))
  check('no text leaked alongside', r.text === '', JSON.stringify(r.text))
}
{
  // Two calls announced together with a SPARSE index (2 arrives before 0).
  //
  // This was the duplicate-call bug: the fold compacted its array with
  // filter(Boolean), shifting call_B from index 2 to position 1, while the stream kept
  // index 2. The finaliser compared position 1 against "nothing arrived at 1" and
  // restated the call, so the client held three calls and call_B's arguments twice.
  const r = await ask({ scenario: 'sparse_indices', tools: TOOLS })
  const byName = Object.fromEntries(r.calls.map((c) => [c.name, parse(c.args)]))
  check('sparse upstream indices yield exactly two calls, not three', r.calls.length === 2,
    JSON.stringify(r.calls.map((c) => `${c.name}@${c.id}`)))
  // Assert by NAME: which call arrives first is upstream's choice, not a contract.
  check('call A kept its own arguments (the indices did not swap)',
    String(byName.bash?.command).trim() === 'echo A', JSON.stringify(byName.bash))
  check('call B kept its own arguments',
    String(byName.grep?.pattern).trim() === 'B', JSON.stringify(byName.grep))
  check('each argument blob is exactly one JSON document, not two run together',
    r.calls.every((c) => c.args.trim().startsWith('{') && c.args.trim().endsWith('}') && c.args.indexOf('}{') === -1),
    JSON.stringify(r.calls.map((c) => c.args.slice(0, 60))))
  check('the two calls sit at distinct indices', new Set(r.calls.map((c) => c.id)).size === r.calls.length)
}
{
  const r = await ask({ scenario: 'tool_big_args', tools: TOOLS }, 120000)
  const a = parse(r.calls[0]?.args)
  check('a 120KB argument survives the round trip', Boolean(a?.command?.startsWith('echo xxx')), 'len ' + (r.calls[0]?.args || '').length)
  check('and is neither truncated nor duplicated', a?.command?.length === 120005, String(a?.command?.length))
}
{
  const r = await ask({ scenario: 'text_and_tool', tools: TOOLS })
  check('text preceding a tool call is delivered', r.text === 'Let me check. ', JSON.stringify(r.text))
  check('and the tool call arrives alongside it', parse(r.calls[0]?.args)?.command === 'date', JSON.stringify(r.calls[0]?.args))
}

// ---------------------------------------------------------------- misbehaving upstream
group('a misbehaving upstream produces a typed failure, never a hang or a fake success')
{
  const r = await ask({ scenario: 'die_midstream' })
  check('a mid-stream reset does not hang', r.ms < 30000, r.ms + 'ms')
  check('and is not reported as success', !r.sawDone || r.err, JSON.stringify({ done: r.sawDone, err: r.err }))
}
{
  // The provider's own words, not just a kind. Only the non-streaming path used to
  // include them, so every harness (which streams) was told merely "(transport)".
  const r = await ask({ scenario: 'http_500' })
  check('HTTP 500 surfaces as an error, not an empty success', Boolean(r.err) || r.status !== 200, JSON.stringify({ s: r.status, err: r.err }))
  check('a streamed error names the real upstream message', (r.err || '').includes('provider exploded'), String(r.err))
}
{
  const r = await ask({ scenario: 'http_400' })
  check('HTTP 400 surfaces the provider message', (r.err || '').includes('tools[4].type'), String(r.err))
}
check('a free-tier gate is reported promptly rather than retried forever',
  (await ask({ scenario: 'http_403_gate' })).ms < 40000)
{
  const r = await ask({ scenario: 'empty_200' }, 90000)
  check('an empty 200 is not served as a successful answer', Boolean(r.err) || r.text === '',
    JSON.stringify({ status: r.status, text: r.text, err: r.err }))
}
{
  const r = await ask({ scenario: 'garbage' })
  check('a non-SSE garbage body produces no invented content', r.text === '', JSON.stringify(r.text))
  check('and does not take the relay down', Boolean(r.err) || r.status === 200)
}
check('a stream that ends without [DONE] still terminates the client',
  (await ask({ scenario: 'stream_truncated' })).ms < 40000)

group('a withdrawn model is refused clearly and cheaply')
{
  // The provider retired the model. Retrying across three lanes cannot help, so the
  // client must be told the real reason and must not pay for six upstream calls.
  // Uses its own model: the quarantine is global and lasts an hour, so reusing
  // big-pickle here would hand every later test a 503.
  //
  // It must also still be IN THE CATALOGUE. This used muse-spark-1.2-contributor-free,
  // which was removed from models.js when the provider withdrew it -- so the relay
  // answered "unknown model" without calling upstream, nothing was quarantined, and
  // three checks here failed for a reason that had nothing to do with `gone`. A test
  // model that stops existing silently turns this group into a no-op.
  const VICTIM = 'glm-5.3-flash-free'
  const r = await ask({ scenario: 'http_410_gone', model: VICTIM })
  check('the client is told the model was withdrawn', /withdrew|gone|no longer/i.test(r.err || ''), String(r.err))
  check('and the answer arrives promptly, without a retry storm', r.ms < 20000, r.ms + 'ms')
  const h = await (await fetch(BASE + '/health')).json()
  const mh = (h.modelHealth || {})[VICTIM] || {}
  check('the model is quarantined immediately, not after three more requests', mh.quarantined === true, JSON.stringify(mh))
  check('no lane was retired for a model-side problem', h.retired === 0, 'retired=' + h.retired)
  // Half-open: the model must be probed again eventually rather than refused forever.
  check('the quarantine is bounded, not permanent', mh.retryInS > 0 && mh.retryInS <= 3600, JSON.stringify(mh))
}

// ---------------------------------------------------------------- lifecycle
group('concurrency and lifecycle')
{
  const many = await Promise.all(Array.from({ length: 12 }, () => ask({ scenario: 'plain' })))
  check('12 concurrent streams all carry the exact text', many.every((r) => r.text === 'Hello world'),
    JSON.stringify(many.map((r) => r.text).filter((t) => t !== 'Hello world')))
  check('no 429 reaches the client', many.every((r) => r.status === 200), JSON.stringify(many.map((r) => r.status)))
}
{
  const mixed = await Promise.all([
    ...Array.from({ length: 4 }, () => ask({ scenario: 'tool_small_deltas', tools: TOOLS })),
    ...Array.from({ length: 4 }, () => ask({ scenario: 'plain' })),
    ...Array.from({ length: 4 }, (_, i) => ask({ scenario: i % 2 ? 'http_500' : 'plain' })),
  ])
  check('tool calls stay correct while neighbouring requests fail',
    mixed.slice(0, 4).every((r) => parse(r.calls[0]?.args)?.command === 'ls -la /tmp'),
    JSON.stringify(mixed.slice(0, 4).map((r) => r.calls?.[0]?.args)))
  check('healthy requests are unaffected by failing neighbours',
    mixed.filter((r) => r.text === 'Hello world').length >= 4,
    JSON.stringify(mixed.map((r) => r.text).filter(Boolean)))
}
{
  const ac = new AbortController()
  const p = fetch(BASE + '/v1/chat/completions', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'github/big-pickle', stream: true, messages: [{ role: 'user', content: '[[S:one_shot]] go' }] }),
    signal: ac.signal,
  }).then((r) => r.body.getReader().read()).catch(() => 'aborted')
  setTimeout(() => ac.abort(), 5)
  await p
  await sleep(800)
  check('the relay still serves after a client abort',
    (await ask({ scenario: 'plain' })).text === 'Hello world')
  const h = await (await fetch(BASE + '/health')).json()
  check('no work is stranded in the queue after an abort', h.queue.pending === 0, JSON.stringify(h.queue))
}

// ---------------------------------------------------------------- direct mode
// DIRECT_MODE bypasses the lane pool entirely: the relay calls the upstream from its own
// host. Verified here with ZERO lanes registered, which is the whole point -- if any of
// these pass because a lane happened to serve them, the test proves nothing.
{
  console.log('\ndirect mode: no lanes, no orchestrator')
  const R = Number(process.env.DIRECT_PORT || PORT + 1)
  const rel = spawn(process.execPath, [path.join(SRC, 'main.js')], {
    env: { ...env, PORT: String(R), HOST: '127.0.0.1', RELAY_TOKEN: 't', DIRECT_MODE: '1', DIRECT_FALLBACK: '0', ZEN_BASE: ZEN },
    stdio: 'ignore',
  })
  kids.push(rel)
  const DBASE = `http://127.0.0.1:${R}`
  const up = await waitFor(async () => (await fetch(DBASE + '/health', { signal: AbortSignal.timeout(2000) })).ok)
  check('the relay starts in direct mode', up)
  if (up) {
    const h = await (await fetch(DBASE + '/health')).json()
    check('with ZERO lanes registered', h.lanes === 0, `lanes=${h.lanes}`)
    check('and direct mode reported on', h.direct?.enabled === true, JSON.stringify(h.direct))
    const direct = await ask({ base: DBASE, scenario: 'plain' })
    check('a plain request is served with no lane at all', direct.text === 'Hello world', JSON.stringify(direct.text))
    const tools = await ask({ base: DBASE, scenario: 'tool_small_deltas', tools: TOOLS })
    check('a tool call is served with no lane at all',
      tools.calls.length === 1 && parse(tools.calls[0]?.args)?.command === 'ls -la /tmp',
      JSON.stringify(tools.calls[0]?.args))
    check('and the streamed tool name is not doubled', tools.calls[0]?.name === 'bash', JSON.stringify(tools.calls[0]?.name))
  }
  try { rel.kill('SIGKILL') } catch { /* already gone */ }
  kids.pop()
  await sleep(500)
}

killKids()
stopMock()
console.log(`\n${pass} passed, ${fail} failed`)
process.exit(fail ? 1 : 0)