// End-to-end smoke test against a real relay process and a fake lane.
//
// The fake lane speaks the real /lane/* protocol, so this exercises the actual
// dispatch, streaming, tool-merge and retry paths rather than a mock of them.
//
//   node test/smoke.mjs
//
// Requires nothing but a Node runtime: no GitHub, no upstream, no credentials.

import { spawn } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const PORT = 8799
const TOKEN = 'smoketest'
const BASE = `http://127.0.0.1:${PORT}`
const H = { 'Content-Type': 'application/json', 'x-relay-token': TOKEN }

let failures = 0
const check = (name, cond, detail = '') => {
  if (cond) console.log(`  PASS  ${name}`)
  else { failures++; console.log(`  FAIL  ${name}${detail ? ` -- ${detail}` : ''}`) }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const relay = spawn(process.execPath, [path.join(ROOT, 'src/main.js')], {
  env: {
    ...process.env,
    PORT: String(PORT),
    HOST: '127.0.0.1',
    RELAY_TOKEN: TOKEN,
    GH_TOKEN: '',
    GH_REPO: '',
    MAX_LANES: '40',
    MODEL_QUARANTINE_MS: '800',
    MODEL_HEALTH_THRESHOLD: '3',
  },
  stdio: ['ignore', 'pipe', 'pipe'],
})
relay.stdout.on('data', (d) => process.env.SMOKE_VERBOSE && process.stdout.write(`[relay] ${d}`))
relay.stderr.on('data', (d) => process.env.SMOKE_VERBOSE && process.stderr.write(`[relay!] ${d}`))

const post = async (p, body) => {
  const r = await fetch(BASE + p, { method: 'POST', headers: H, body: JSON.stringify(body) })
  return { status: r.status, body: await r.json().catch(() => null) }
}

/** A lane that answers claims with scripted behaviour. */
class FakeLane {
  constructor (id, script) {
    this.id = id
    this.script = script
    this.served = 0
    this.stopped = true      // explicit start(), so a lane cannot claim early
    this.done = false
    this.loops = 0
  }

  async register () {
    const r = await post('/lane/register', { laneId: this.id })
    if (r.status === 410) throw new Error(`lane ${this.id} is retired`)
    return r.body
  }

  start () {
    this.stopped = false
    this.loop = this.run()
    return this.loop
  }

  /** Stop claiming AND wait for the in-flight claim loop to actually exit. */
  async stop () {
    this.stopped = true
    await this.loop?.catch(() => {})
    // Let the relay notice the lane is idle before the next test picks lanes.
    await sleep(50)
  }

  async run () {
    while (!this.stopped) {
      let claim
      try {
        const r = await fetch(`${BASE}/lane/claim?laneId=${encodeURIComponent(this.id)}&wait=300`, { headers: H })
        if (this.stopped) return
        if (r.status === 204) continue
        if (!r.ok) { await sleep(50); continue }
        claim = await r.json()
      } catch { await sleep(50); continue }
      if (this.stopped) return
      if (!claim?.entryId) continue
      if (this.done) continue
      this.served++
      await this.script(this, claim, claim.job, this.served - 1)
    }
  }

  async delta (entryId, body) {
    return post('/lane/delta', { laneId: this.id, entryId, ...body })
  }

  async result (entryId, result) {
    return post('/lane/result', { laneId: this.id, entryId, result })
  }
}

const sse = async (body) => {
  const r = await fetch(BASE + '/v1/chat/completions', {
    method: 'POST', headers: H, body: JSON.stringify({ ...body, stream: true }),
  })
  const text = await r.text()
  const chunks = text.split('\n\n').filter(Boolean)
    .map((f) => f.replace(/^data: /, ''))
    .filter((f) => f && f !== '[DONE]')
    .map((f) => { try { return JSON.parse(f) } catch { return null } })
    .filter(Boolean)
  return { status: r.status, chunks, raw: text }
}

const main = async () => {
  // Wait for the relay to accept connections.
  let up = false
  for (let i = 0; i < 100; i++) {
    try { const r = await fetch(BASE + '/health'); if (r.ok) { up = true; break } } catch { /* retry */ }
    await sleep(100)
  }
  if (!up) { console.error('relay did not start'); relay.kill(); process.exit(1) }
  console.log('\nrelay is up\n')

  // ---------------------------------------------------------------- models
  console.log('models')
  const models = await (await fetch(BASE + '/v1/models')).json()
  check('advertises github/<model> ids', models.data.every((m) => m.id.startsWith('github/')))
  check('no upstream vendor in payload', !JSON.stringify(models).match(/opencode|zen/i))

  const health = await (await fetch(BASE + '/health')).json()
  check('health names provider only as github', health.provider === 'github' &&
    !JSON.stringify(health).match(/opencode|zen/i))

  // ------------------------------------------------------------ unknown model
  const bad = await post('/v1/chat/completions', { model: 'github/nope', messages: [{ role: 'user', content: 'hi' }] })
  check('unknown model -> 404', bad.status === 404)

  // ============================================================ test 1
  // Text streaming, no tools: no duplication, [DONE] present.
  console.log('\nstreaming text, no duplication')
  {
    const lane = new FakeLane('lane-text', async (l, claim) => {
      await l.delta(claim.entryId, { text: 'Hello ' })
      await l.delta(claim.entryId, { text: 'world' })
      await l.result(claim.entryId, {
        kind: 'ok', status: 200, ms: 10, streamed: true, streamedText: true, streamedTools: false,
        data: { choices: [{ index: 0, message: { role: 'assistant', content: 'Hello world' }, finish_reason: 'stop' }] },
      })
    })
    await lane.register()
    lane.start()
    const { chunks, raw } = await sse({ model: 'github/big-pickle', messages: [{ role: 'user', content: 'hi' }] })
    const content = chunks.map((c) => c.choices?.[0]?.delta?.content || '').join('')
    check('text delivered exactly once', content === 'Hello world', JSON.stringify(content))
    check('[DONE] emitted', raw.trimEnd().endsWith('data: [DONE]'))
    const finishes = chunks.filter((c) => c.choices?.[0]?.finish_reason)
    check('exactly one finish_reason', finishes.length === 1, `${finishes.length}`)
    check('finish_reason is stop', finishes[0]?.choices?.[0]?.finish_reason === 'stop')
    await lane.stop()
  }

  // ============================================================ test 2
  // Tool-only turn: the client must receive the call with id, name, and args --
  // this is the reported `edit` failure.
  console.log('\ntool-only turn reaches the client intact')
  {
    const args = JSON.stringify({ file_path: '/tmp/a.txt', old_string: 'x', new_string: 'y' })
    const lane = new FakeLane('lane-tool', async (l, claim) => {
      // Identity and arguments deliberately arrive in SEPARATE delta batches,
      // which is what upstream does and what the old merge lost.
      await l.delta(claim.entryId, {
        toolCalls: [{ index: 0, id: 'call_abc', type: 'function', function: { name: 'edit', arguments: '' } }],
      })
      for (const piece of [args.slice(0, 12), args.slice(12)]) {
        await l.delta(claim.entryId, { toolCalls: [{ index: 0, function: { arguments: piece } }] })
      }
      await l.result(claim.entryId, {
        kind: 'ok', status: 200, ms: 10, streamed: true, streamedText: false, streamedTools: true,
        data: {
          choices: [{
            index: 0, finish_reason: 'tool_calls',
            message: { role: 'assistant', content: '', tool_calls: [{ id: 'call_abc', type: 'function', function: { name: 'edit', arguments: args } }] },
          }],
        },
      })
    })
    await lane.register()
    lane.start()
    const { chunks } = await sse({ model: 'github/big-pickle', messages: [{ role: 'user', content: 'edit it' }], tools: [{ type: 'function', function: { name: 'edit', parameters: { type: 'object', properties: {} } } }] })

    // Fold the client's view exactly as a harness would: one entry per index,
    // args concatenated.
    const byIndex = new Map()
    for (const c of chunks) {
      for (const tc of c.choices?.[0]?.delta?.tool_calls || []) {
        const i = tc.index ?? 0
        const cur = byIndex.get(i) || { id: undefined, name: '', args: '' }
        if (tc.id) cur.id = tc.id
        if (tc.function?.name) cur.name += tc.function.name
        if (tc.function?.arguments) cur.args += tc.function.arguments
        byIndex.set(i, cur)
      }
    }
    const calls = [...byIndex.values()]
    check('exactly one tool call', calls.length === 1, `${calls.length}`)
    const c0 = calls[0] || {}
    check('id present', c0.id === 'call_abc', String(c0.id))
    check('name present and not duplicated', c0.name === 'edit', JSON.stringify(c0.name))
    let parsed = null
    try { parsed = JSON.parse(c0.args) } catch { /* reported below */ }
    check('arguments are valid JSON', parsed !== null, JSON.stringify(c0.args))
    check('file_path present', parsed?.file_path === '/tmp/a.txt')
    check('old_string present', parsed?.old_string === 'x')
    check('new_string present', parsed?.new_string === 'y')
    const fr = chunks.filter((c) => c.choices?.[0]?.finish_reason).map((c) => c.choices[0].finish_reason)
    check('finish_reason tool_calls', fr.includes('tool_calls'), JSON.stringify(fr))
    await lane.stop()
  }

  // ============================================================ test 3
  // A LOST delta batch: the lane streams half the arguments, then reports the
  // full call. The relay must repair the client's copy, not leave it truncated.
  console.log('\nlost delta batch is repaired')
  {
    const full = JSON.stringify({ file_path: '/tmp/b.txt', old_string: 'aaa', new_string: 'bbb' })
    const half = full.slice(0, 10)
    const lane = new FakeLane('lane-lost', async (l, claim) => {
      await l.delta(claim.entryId, { toolCalls: [{ index: 0, id: 'call_lost', function: { name: 'edit', arguments: half } }] })
      // The remaining batches never arrive (simulated drop). The result still
      // carries the complete folded call.
      await l.result(claim.entryId, {
        kind: 'ok', status: 200, ms: 10, streamed: true, streamedText: false, streamedTools: true,
        data: {
          choices: [{
            index: 0, finish_reason: 'tool_calls',
            message: { role: 'assistant', content: '', tool_calls: [{ id: 'call_lost', type: 'function', function: { name: 'edit', arguments: full } }] },
          }],
        },
      })
    })
    await lane.register()
    lane.start()
    const { chunks } = await sse({ model: 'github/big-pickle', messages: [{ role: 'user', content: 'edit' }] })
    const byIndex = new Map()
    for (const c of chunks) {
      for (const tc of c.choices?.[0]?.delta?.tool_calls || []) {
        const i = tc.index ?? 0
        const cur = byIndex.get(i) || { name: '', args: '' }
        if (tc.function?.name && !cur.name) cur.name = tc.function.name
        if (tc.function?.arguments) cur.args += tc.function.arguments
        byIndex.set(i, cur)
      }
    }
    const c0 = [...byIndex.values()][0] || {}
    let parsed = null
    try { parsed = JSON.parse(c0.args) } catch { /* reported */ }
    check('truncated arguments repaired to valid JSON', parsed !== null, JSON.stringify(c0.args))
    check('repaired call has file_path', parsed?.file_path === '/tmp/b.txt')
    check('name still not duplicated', c0.name === 'edit', JSON.stringify(c0.name))
    await lane.stop()
  }

  // ============================================================ test 4
  // Non-streaming tool call: full call in one response.
  console.log('\nnon-streaming tool call')
  {
    const args = JSON.stringify({ file_path: '/tmp/c.txt', old_string: 'p', new_string: 'q' })
    const lane = new FakeLane('lane-nonstream', async (l, claim) => {
      await l.result(claim.entryId, {
        kind: 'ok', status: 200, ms: 10, streamed: false, streamedText: false, streamedTools: false,
        data: {
          choices: [{
            index: 0, finish_reason: 'tool_calls',
            message: { role: 'assistant', content: '', tool_calls: [{ id: 'call_ns', type: 'function', function: { name: 'edit', arguments: args } }] },
          }],
        },
      })
    })
    await lane.register()
    lane.start()
    const r = await post('/v1/chat/completions', { model: 'github/big-pickle', messages: [{ role: 'user', content: 'edit' }] })
    const msg = r.body?.choices?.[0]?.message
    check('200 for non-streaming', r.status === 200, String(r.status))
    check('tool_calls present', Array.isArray(msg?.tool_calls) && msg.tool_calls.length === 1)
    check('non-streaming args intact', msg?.tool_calls?.[0]?.function?.arguments === args)
    check('finish_reason tool_calls', r.body?.choices?.[0]?.finish_reason === 'tool_calls')
    await lane.stop()
  }

  // ============================================================ test 5
  // Empty upstream answer is retried on another lane, not returned blank.
  console.log('\nempty answer retried on a different lane')
  {
    const seen = new Set()
    const mk = (id, empty) => new FakeLane(id, async (l, claim) => {
      seen.add(id)
      await l.result(claim.entryId, empty
        ? { kind: 'empty', status: 200, ms: 5, streamed: false, data: { choices: [{ index: 0, message: { role: 'assistant', content: '' }, finish_reason: 'stop' }] } }
        : { kind: 'ok', status: 200, ms: 5, streamed: false, data: { choices: [{ index: 0, message: { role: 'assistant', content: 'recovered' }, finish_reason: 'stop' }] } })
    })
    const a = mk('lane-empty', true)
    const b = mk('lane-good', false)
    await a.register(); await b.register()
    a.start(); b.start()
    const r = await post('/v1/chat/completions', { model: 'github/mimo-v2.5-free', messages: [{ role: 'user', content: 'hi' }] })
    check('client got the healthy answer', r.body?.choices?.[0]?.message?.content === 'recovered',
      JSON.stringify(r.body?.choices?.[0]?.message?.content))
    check('a second lane was used', seen.size === 2, [...seen].join(','))
    await a.stop(); await b.stop()
  }

  // ============================================================ test 6
  // Limited (429 upstream) retires the lane, requeues the work, client sees 200.
  console.log('\nupstream 429 retires the lane and never reaches the client')
  {
    let limitedSeen = false
    const burned = new FakeLane('lane-burned', async (l, claim) => {
      limitedSeen = true
      await l.result(claim.entryId, { kind: 'limited', status: 429, ms: 5, streamed: false, raw: 'FreeUsageLimitError' })
    })
    const good = new FakeLane('lane-fresh', async (l, claim) => {
      await l.result(claim.entryId, { kind: 'ok', status: 200, ms: 5, streamed: false, data: { choices: [{ index: 0, message: { role: 'assistant', content: 'served by fresh lane' }, finish_reason: 'stop' }] } })
    })
    await burned.register(); await good.register()
    burned.start(); good.start()
    const r = await post('/v1/chat/completions', { model: 'github/mimo-v2.6-flash-free', messages: [{ role: 'user', content: 'hi' }] })
    check('client status is not 429', r.status !== 429, String(r.status))
    check('client got a real answer', r.body?.choices?.[0]?.message?.content === 'served by fresh lane',
      JSON.stringify(r.body?.choices?.[0]?.message?.content))
    check('the exhausted lane did serve once', limitedSeen)
    // The retired lane must not be able to reclaim work.
    const reReg = await post('/lane/register', { laneId: 'lane-burned' })
    check('retired lane cannot re-register', reReg.status === 410, String(reReg.status))
    await burned.stop(); await good.stop()
  }

  // ============================================================ test 7
  // Quarantine expires: a model that failed transiently recovers on its own.
  console.log('\nmodel quarantine expires (half-open)')
  {
    const bad = new FakeLane('lane-q1', async (l, claim) => {
      await l.result(claim.entryId, { kind: 'provider_error', status: 400, ms: 5, streamed: false, raw: 'Endpoint is unavailable' })
    })
    await bad.register(); bad.start()
    const model = 'github/nemotron-3-ultra-free'
    const body = { model, messages: [{ role: 'user', content: 'hi' }] }
    let sawQuarantine = false
    for (let i = 0; i < 4; i++) {
      const r = await post('/v1/chat/completions', body)
      if (r.status === 503) { sawQuarantine = true; break }
    }
    check('model gets quarantined after repeated hard failures', sawQuarantine)
    await bad.stop()
    // Wait out QUARANTINE_MS (800ms in this run) and let a healthy lane take over.
    await sleep(1200)
    const good = new FakeLane('lane-q2', async (l, claim) => {
      await l.result(claim.entryId, { kind: 'ok', status: 200, ms: 5, streamed: false, data: { choices: [{ index: 0, message: { role: 'assistant', content: 'back online' }, finish_reason: 'stop' }] } })
    })
    await good.register(); good.start()
    const r = await post('/v1/chat/completions', body)
    check('quarantine expires and the model serves again', r.status === 200,
      `${r.status} ${JSON.stringify(r.body?.error?.message || '')}`)
    await good.stop()
  }

  // ============================================================ test 8
  // Queue hygiene: after everything settles, nothing is left pending or in flight.
  console.log('\nqueue is empty after all work settles')
  {
    await sleep(600)
    const h = await (await fetch(BASE + '/health')).json()
    check('no pending entries', h.queue.pending === 0, String(h.queue.pending))
    check('no in-flight entries', h.queue.inflight === 0, String(h.queue.inflight))
    const streamed = h.queue.pending === 0 && h.queue.inflight === 0
    check('queue drained', streamed)
  }

  console.log(`\n${failures === 0 ? 'ALL CHECKS PASSED' : `${failures} CHECK(S) FAILED`}\n`)
  relay.kill('SIGTERM')
  await sleep(200)
  process.exit(failures === 0 ? 0 : 1)
}

main().catch((e) => {
  console.error('smoke test crashed:', e)
  relay.kill('SIGTERM')
  process.exit(1)
})
