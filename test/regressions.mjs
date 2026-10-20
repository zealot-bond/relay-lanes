// Regression tests for defects found by independent review of the relay.
//
//   node test/regressions.mjs                       # test ../src
//   SRC_DIR=/path/to/older/src node test/regressions.mjs   # prove a test FAILS on old code
//
// Every scenario drives the REAL modules over loopback (real HTTP server, real
// raw sockets, a real TLS mock for the upstream) rather than re-implementing
// their logic. Each runs in its own process because the relay keeps module-level
// state (queue, lane registry, config read at import).
//
// A test that cannot fail is worthless, so the suite is meant to be pointed at the
// pre-fix tree as well: the expected result there is a FAILURE for each scenario.
import { spawnSync, execFileSync } from 'node:child_process'
import fs from 'node:fs'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import https from 'node:https'
import { fileURLToPath, pathToFileURL } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const SRC = path.resolve(process.env.SRC_DIR || path.join(here, '..', 'src'))
const imp = (f) => import(pathToFileURL(path.join(SRC, f)).href)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const H = { 'content-type': 'application/json', 'x-relay-token': 'relay' }

const bootRelay = async (port, env = {}) => {
  process.env.PORT = String(port)
  process.env.HOST = '127.0.0.1'
  // These scenarios exercise the QUEUE and LANE machinery. DIRECT_MODE now defaults to 1
  // and would bypass it entirely, making them vacuous.
  process.env.DIRECT_MODE = '0'
  process.env.DIRECT_FALLBACK = '0'   // lanes-only: the backup must not mask a lane bug
  Object.assign(process.env, env)
  const S = await imp('server.js')
  S.start()
  await sleep(250)
  const B = `http://127.0.0.1:${port}`
  const post = (p, b) => fetch(B + p, { method: 'POST', headers: H, body: JSON.stringify(b) }).then((r) => r.json())
  return { S, B, post }
}

const chatChunk = (text) => `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: text } }] })}\n\n`
const mark = (i, sz) => `<<${String(i).padStart(3, '0')}>>` + 'x'.repeat(sz)

// ---------------------------------------------------------------- scenarios
// Each returns [{ name, ok, detail }].
const scenarios = {
  // #1 write() returning false was treated as "not written", so the chunk was
  // queued AND already sent: every drain re-sent it.
  async 'backpressure: no duplicated frames' () {
    // Volume matters: loopback kernel buffers absorb a couple of MB before write()
    // reports pressure, so a small N never reaches the code under test and passes on
    // the broken version too (it did, at N=30).
    const N = 100, SZ = 60000
    const { B, post } = await bootRelay(18901, { MAX_BACKLOG_BYTES: '1000000000' })
    await post('/lane/register', { laneId: 'L1' })
    const body = JSON.stringify({ model: 'github/big-pickle', stream: true, messages: [{ role: 'user', content: 'hi' }] })
    const sock = net.connect(18901, '127.0.0.1')
    const buf = []
    sock.on('data', (d) => buf.push(d))
    sock.write(`POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`)
    const claim = await fetch(B + '/lane/claim?laneId=L1&wait=3000', { headers: H }).then((r) => r.json())
    sock.pause()
    for (let i = 0; i < N; i++) await post('/lane/delta', { laneId: 'L1', entryId: claim.entryId, text: mark(i, SZ) })
    sock.resume()
    await sleep(500)
    await post('/lane/result', { laneId: 'L1', entryId: claim.entryId, result: { kind: 'ok', status: 200, ms: 5, streamed: true, streamedText: true,
      data: { choices: [{ index: 0, message: { role: 'assistant', content: Array.from({ length: N }, (_, i) => mark(i, SZ)).join('') }, finish_reason: 'stop' }] } } })
    await sleep(1500)
    const marks = [...Buffer.concat(buf).toString().matchAll(/<<(\d{3})>>/g)].map((m) => m[1])
    const dups = marks.length - new Set(marks).size
    return [
      { name: 'every delta arrives exactly once', ok: marks.length === N && dups === 0, detail: `got ${marks.length} frames, ${dups} duplicated, expected ${N}` },
      { name: 'frames arrive in order', ok: marks.every((m, i) => Number(m) === i), detail: marks.slice(0, 8).join(',') },
    ]
  },

  // #2 res.end() ran while data was still queued, dropping the tail.
  async 'backpressure: the tail is not lost' () {
    const N = 80, SZ = 60000
    const { B, post } = await bootRelay(18902, { MAX_BACKLOG_BYTES: '1000000000' })
    await post('/lane/register', { laneId: 'L1' })
    const body = JSON.stringify({ model: 'github/big-pickle', stream: true, messages: [{ role: 'user', content: 'hi' }] })
    const sock = net.connect(18902, '127.0.0.1')
    const buf = []
    sock.on('data', (d) => buf.push(d))
    sock.write(`POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`)
    const claim = await fetch(B + '/lane/claim?laneId=L1&wait=3000', { headers: H }).then((r) => r.json())
    sock.pause()
    for (let i = 0; i < N; i++) await post('/lane/delta', { laneId: 'L1', entryId: claim.entryId, text: mark(i, SZ) })
    await sleep(400)
    // The result lands while the client is STILL not reading.
    await post('/lane/result', { laneId: 'L1', entryId: claim.entryId, result: { kind: 'ok', status: 200, ms: 5, streamed: true, streamedText: true,
      data: { choices: [{ index: 0, message: { role: 'assistant', content: Array.from({ length: N }, (_, i) => mark(i, SZ)).join('') }, finish_reason: 'stop' }] } } })
    sock.resume()
    await sleep(1500)
    const all = Buffer.concat(buf).toString()
    const marks = [...all.matchAll(/<<(\d{3})>>/g)].map((m) => m[1])
    return [
      { name: 'all frames delivered', ok: marks.length === N, detail: `${marks.length}/${N}` },
      { name: 'stream ends with finish_reason and [DONE]', ok: all.includes('"finish_reason":"stop"') && all.includes('[DONE]'), detail: `stop=${all.includes('"finish_reason":"stop"')} done=${all.includes('[DONE]')}` },
    ]
  },

  // #3 the reaper woke the dispatcher, whose requeue() deleted the NEW lane's claim.
  async 'reaper: a live claim is not stolen' () {
    const { S, B, post } = await bootRelay(18903, { LANE_STALE_MS: '600', CLAIM_GRACE_MS: '100', ATTEMPT_HARD_MS: '60000' })
    S.queue.claimLeaseMs = 300
    await post('/lane/register', { laneId: 'L1' })
    fetch(B + '/v1/chat/completions', { method: 'POST', headers: H, body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }) }).catch(() => {})
    const c1 = await fetch(B + '/lane/claim?laneId=L1&wait=3000', { headers: H }).then((r) => r.json())
    await post('/lane/register', { laneId: 'L2' })
    const hb = setInterval(() => post('/lane/heartbeat', { laneId: 'L2' }), 150)
    const c2 = await fetch(B + '/lane/claim?laneId=L2&wait=8000', { headers: H }).then((r) => (r.status === 204 ? null : r.json()))
    await sleep(500)
    const stillHeld = S.queue.inflight.has(c1.entryId) && S.queue.pending.length === 0
    const r2 = await post('/lane/result', { laneId: 'L2', entryId: c2.entryId, result: { kind: 'ok', status: 200, ms: 5, data: { choices: [{ index: 0, message: { role: 'assistant', content: 'answer' }, finish_reason: 'stop' }] } } })
    clearInterval(hb)
    return [
      { name: 'the new lane still holds its claim after the dispatcher wakes', ok: stillHeld, detail: `inflight=${S.queue.inflight.has(c1.entryId)} pending=${S.queue.pending.length}` },
      { name: "the new lane's result is accepted", ok: r2.ok === true, detail: JSON.stringify(r2) },
    ]
  },

  // #4 a lane died after text was streamed; the entry was served again from the top.
  async 'reaper: a committed stream is not replayed' () {
    const { S, B, post } = await bootRelay(18904, { LANE_STALE_MS: '600', CLAIM_GRACE_MS: '100', ATTEMPT_HARD_MS: '60000' })
    S.queue.claimLeaseMs = 300
    await post('/lane/register', { laneId: 'L1' })
    const client = fetch(B + '/v1/chat/completions', { method: 'POST', headers: H, body: JSON.stringify({ model: 'big-pickle', stream: true, messages: [{ role: 'user', content: 'hi' }] }) }).then((r) => r.text())
    const c1 = await fetch(B + '/lane/claim?laneId=L1&wait=3000', { headers: H }).then((r) => r.json())
    await post('/lane/delta', { laneId: 'L1', entryId: c1.entryId, text: 'hello ' })
    await post('/lane/register', { laneId: 'L2' })
    const hb = setInterval(() => post('/lane/heartbeat', { laneId: 'L2' }), 150)
    const c2 = await fetch(B + '/lane/claim?laneId=L2&wait=2500', { headers: H }).then((r) => (r.status === 204 ? null : r.json()))
    clearInterval(hb)
    const out = await Promise.race([client, sleep(6000).then(() => '(client still hanging)')])
    const texts = [...out.matchAll(/"content":"([^"]*)"/g)].map((m) => m[1]).filter(Boolean)
    return [
      { name: 'a second lane is not handed the already-streamed entry', ok: c2 === null || c2.entryId !== c1.entryId, detail: `second claim: ${c2 ? c2.entryId : 'none'}` },
      { name: 'the client text is not replayed', ok: texts.join('') === 'hello ', detail: JSON.stringify(texts) },
      { name: 'the client stream terminates', ok: out !== '(client still hanging)', detail: out.slice(-60).replace(/\n/g, ' ') },
    ]
  },

  // #5 the claim guard sat inside `if (entry)`, so a delta from a lane with NO claim
  // (entry back in pending) skipped it and was written to the client.
  async 'delta: a lane without a claim cannot write' () {
    const { S, B, post } = await bootRelay(18905, { LANE_STALE_MS: '60000', CLAIM_GRACE_MS: '100' })
    await post('/lane/register', { laneId: 'L1' })
    const client = fetch(B + '/v1/chat/completions', { method: 'POST', headers: H, body: JSON.stringify({ model: 'big-pickle', stream: true, messages: [{ role: 'user', content: 'hi' }] }) }).then((r) => r.text())
    const c1 = await fetch(B + '/lane/claim?laneId=L1&wait=3000', { headers: H }).then((r) => r.json())
    S.queue.requeue(S.queue.inflight.get(c1.entryId))
    const r = await post('/lane/delta', { laneId: 'L1', entryId: c1.entryId, text: 'GHOST FROM LANE THAT LOST CLAIM' })
    S.queue.abandon(S.queue.pending[0])
    const out = await Promise.race([client, sleep(2000).then(() => '')])
    return [
      { name: 'the delta is rejected', ok: r.ok === false, detail: JSON.stringify(r) },
      { name: 'the ghost text never reaches the client', ok: !out.includes('GHOST'), detail: out.includes('GHOST') ? 'DELIVERED' : 'not delivered' },
    ]
  },

  // #9 health was counted per ATTEMPT, so two bad requests from one client
  // quarantined the model for everyone.
  async 'health: counted once per request' () {
    const { B, post } = await bootRelay(18906)
    const ids = ['A', 'B', 'C', 'D']
    for (const l of ids) await post('/lane/register', { laneId: l })
    for (const l of ids) {
      (async () => {
        for (;;) {
          const r = await fetch(B + `/lane/claim?laneId=${l}&wait=1000`, { headers: H })
          if (r.status === 204) continue
          if (r.status !== 200) return
          const c = await r.json()
          await post('/lane/result', { laneId: l, entryId: c.entryId, result: { kind: 'provider_error', status: 400, ms: 1, raw: 'boom', data: null } })
        }
      })()
    }
    const statuses = []
    for (let i = 0; i < 2; i++) {
      const r = await fetch(B + '/v1/chat/completions', { method: 'POST', headers: H, body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }) })
      statuses.push(r.status); await r.text()
    }
    const third = await fetch(B + '/v1/chat/completions', { method: 'POST', headers: H, body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }) })
    const t = third.status; await third.text()
    const h = await fetch(B + '/health').then((r) => r.json())
    return [
      { name: 'two failing requests do not quarantine the model', ok: statuses.every((s) => s === 502), detail: `statuses ${statuses.join(',')}` },
      { name: 'a third request is still attempted (counter is 2, threshold 3)', ok: t === 502, detail: `third=${t} fails=${h.modelHealth?.['big-pickle']?.fails}` },
    ]
  },

  // #10 the cap was checked after requeue(), so a lane served one call too many.
  async 'empty: retries stop at the cap' () {
    const { B, post } = await bootRelay(18907)
    const ids = ['A', 'B', 'C', 'D', 'E', 'F']
    for (const l of ids) await post('/lane/register', { laneId: l })
    let calls = 0
    for (const l of ids) {
      (async () => {
        for (;;) {
          const r = await fetch(B + `/lane/claim?laneId=${l}&wait=1000`, { headers: H })
          if (r.status === 204) continue
          if (r.status !== 200) return
          const c = await r.json(); calls++
          await post('/lane/result', { laneId: l, entryId: c.entryId, result: { kind: 'empty', status: 200, ms: 1, raw: '', data: null } })
        }
      })()
    }
    const r = await fetch(B + '/v1/chat/completions', { method: 'POST', headers: H, body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }) })
    await r.text()
    await sleep(1500)   // anything served AFTER the verdict would show up here
    const limit = Number(process.env.RETRY_LIMIT || 3) + 1
    return [{ name: `exactly ${limit} upstream calls for an always-empty request`, ok: calls === limit, detail: `calls=${calls}` }]
  },

  // A lane on stale code corrupted tool calls ("bashbash") and still got work,
  // because lanes pull from /lane/claim regardless of which one the dispatcher
  // preferred.
  async 'stale lanes: starved while a current lane exists, used as a last resort' () {
    const { CODE_VERSION } = await imp('codeversion.js')
    const { B, post } = await bootRelay(18908, { LANE_STALE_MS: '60000' })
    await post('/lane/register', { laneId: 'CUR', codeVersion: CODE_VERSION })
    await post('/lane/register', { laneId: 'OLD' })                 // unversioned, like pre-fix runners
    const served = { CUR: 0, OLD: 0 }
    const work = (l) => (async () => {
      for (;;) {
        const r = await fetch(B + `/lane/claim?laneId=${l}&wait=1000`, { headers: H }).catch(() => null)
        if (!r) return
        if (r.status === 204) continue
        if (r.status !== 200) return
        const c = await r.json(); served[l]++
        await post('/lane/result', { laneId: l, entryId: c.entryId, result: { kind: 'ok', status: 200, ms: 1, data: { choices: [{ index: 0, message: { role: 'assistant', content: 'hi' }, finish_reason: 'stop' }] } } })
      }
    })()
    work('CUR'); work('OLD')
    const ask = () => fetch(B + '/v1/chat/completions', { method: 'POST', headers: H, body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }) }).then((r) => r.status)
    const statuses = await Promise.all(Array.from({ length: 8 }, ask))
    const whileCurrent = { ...served }

    return [
      { name: 'all requests succeed', ok: statuses.every((s) => s === 200), detail: statuses.join(',') },
      { name: 'the current lane served every request', ok: whileCurrent.CUR === 8, detail: JSON.stringify(whileCurrent) },
      { name: 'the stale lane served none while a current one was live', ok: whileCurrent.OLD === 0, detail: JSON.stringify(whileCurrent) },
    ]
  },

  async 'stale lanes: take over when no current lane exists' () {
    const { B, post } = await bootRelay(18909, { LANE_STALE_MS: '60000' })
    await post('/lane/register', { laneId: 'OLD' })
    let served = 0
    ;(async () => {
      for (;;) {
        const r = await fetch(B + '/lane/claim?laneId=OLD&wait=1000', { headers: H }).catch(() => null)
        if (!r) return
        if (r.status === 204) continue
        if (r.status !== 200) return
        const c = await r.json(); served++
        await post('/lane/result', { laneId: 'OLD', entryId: c.entryId, result: { kind: 'ok', status: 200, ms: 1, data: { choices: [{ index: 0, message: { role: 'assistant', content: 'hi' }, finish_reason: 'stop' }] } } })
      }
    })()
    const r = await fetch(B + '/v1/chat/completions', { method: 'POST', headers: H, body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }) })
    await r.text()
    return [{ name: 'with only stale lanes the request is still served', ok: r.status === 200 && served === 1, detail: `status=${r.status} served=${served}` }]
  },

  // #6 a multi-byte character split across TCP reads was decoded per chunk.
  async 'lane: utf-8 split across reads' () {
    const { stop, port } = await mockUpstream((res) => {
      const b = Buffer.concat([Buffer.from(chatChunk('日本語 😀 ok')), Buffer.from('data: [DONE]\n\n')])
      const i = b.indexOf(Buffer.from('本')) + 1
      res.write(b.subarray(0, i)); setTimeout(() => { res.write(b.subarray(i)); res.end() }, 50)
    })
    process.env.ZEN_BASE = `https://127.0.0.1:${port}`
    const { callUpstream } = await imp('lane.js')
    const got = []
    const r = await callUpstream({ model: 'big-pickle', messages: [{ role: 'user', content: 'x' }], stream: true }, { onDelta: (x) => got.push(x) })
    stop()
    const streamed = got.join('')
    return [
      { name: 'streamed text is intact', ok: streamed === '日本語 😀 ok', detail: JSON.stringify(streamed) },
      { name: 'streamed text equals the folded text', ok: streamed === r.data?.choices?.[0]?.message?.content, detail: JSON.stringify(r.data?.choices?.[0]?.message?.content) },
    ]
  },

  // #7 an upstream reset after the headers waited out the whole idle timer.
  async 'lane: mid-stream reset is detected promptly' () {
    const { stop, port } = await mockUpstream((res) => {
      res.write(chatChunk('partial'))
      setTimeout(() => res.socket.destroy(), 100)
    })
    process.env.ZEN_BASE = `https://127.0.0.1:${port}`
    process.env.STREAM_IDLE_TIMEOUT_MS = '4000'
    const { callUpstream } = await imp('lane.js')
    const t = Date.now()
    const r = await callUpstream({ model: 'big-pickle', messages: [{ role: 'user', content: 'x' }], stream: true }, { onDelta: () => {} })
    const ms = Date.now() - t
    stop()
    return [
      { name: 'resolves long before the 4000ms idle timer', ok: ms < 2000, detail: `${ms}ms` },
      { name: 'reported as a retryable error, not a timeout', ok: r.kind === 'error', detail: `kind=${r.kind}` },
    ]
  },

  // #8 one non-JSON data line poisoned every later frame.
  async 'lane: a stray non-JSON line does not eat the stream' () {
    const lane = await imp('lane.js')
    const raw = chatChunk('A') + 'data: ping\n\n' + chatChunk('B') + chatChunk('C') + 'data: [DONE]\n\n'
    const folded = lane.parseForTest(raw, 'big-pickle')
    const wrapped = lane.parseForTest('data: {"choices":[{"index":0,\ndata: "delta":{"content":"XY"}}]}\n\ndata: [DONE]\n\n', 'big-pickle')
    return [
      { name: 'frames after the stray line survive', ok: folded?.choices?.[0]?.message?.content === 'ABC', detail: JSON.stringify(folded?.choices?.[0]?.message?.content) },
      { name: 'a legitimately wrapped (multi-line) frame still joins', ok: wrapped?.choices?.[0]?.message?.content === 'XY', detail: JSON.stringify(wrapped?.choices?.[0]?.message?.content) },
    ]
  },

  // A genuine rate limit must reach the client AS a 429. It used to be flattened into a
  // 502 whose message ended "This is an upstream fault, not a rate limit" while the body
  // said "FreeUsageLimitError: Rate limit exceeded" -- which sent the operator hunting a
  // code bug instead of waiting out the quota.
  async 'client: a rate limit is reported as 429 with Retry-After' () {
    const { B, post } = await bootRelay(18924, { RATE_LIMIT_RETRY_AFTER_S: '900' })
    // Several lanes: `limited` RETIRES the lane that drew it, so a single-lane pool would
    // leave nothing for the retry and the request would sit until the hold deadline.
    // (That retirement is correct -- that egress IP's bucket is spent.)
    for (const id of ['A', 'B', 'C']) await post('/lane/register', { laneId: id })
    for (const id of ['A', 'B', 'C']) {
      ;(async () => {
        for (;;) {
          const r = await fetch(B + `/lane/claim?laneId=${id}&wait=1000`, { headers: H }).catch(() => null)
          if (!r) return
          if (r.status !== 200) continue
          const c = await r.json()
          await post('/lane/result', { laneId: id, entryId: c.entryId, result: { kind: 'limited', status: 429, ms: 5,
            raw: '{"error":{"type":"FreeUsageLimitError","message":"Rate limit exceeded. Please try again later."}}', data: null } })
        }
      })()
    }
    const res = await fetch(B + '/v1/chat/completions', { method: 'POST', headers: H,
      body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }) })
    const body = await res.json().catch(() => ({}))
    const msg = String(body?.error?.message || '')
    return [
      { name: 'the status is 429, not 502', ok: res.status === 429, detail: 'status ' + res.status },
      { name: 'Retry-After is present', ok: Boolean(res.headers.get('retry-after')), detail: String(res.headers.get('retry-after')) },
      { name: 'the message says it is a rate limit', ok: /rate limit/i.test(msg), detail: msg.slice(0, 90) },
      { name: 'and does NOT claim it is not one', ok: !/not a rate limit/i.test(msg), detail: msg.slice(0, 90) },
      { name: 'it names direct mode as the reason retrying will not help',
        ok: /DIRECT_MODE/.test(msg) || /egress IP/i.test(msg), detail: msg.slice(0, 120) },
    ]
  },

  // /health went through an entire rate-limit storm reporting recentFailures: [] because
  // `limited` was excluded from logging along with the kinds that must not count toward
  // model health. The operator had served=0 and nothing else to go on.
  async 'health: rate limiting is visible on /health' () {
    const { B, post } = await bootRelay(18926)
    for (const id of ['A', 'B']) await post('/lane/register', { laneId: id })
    for (const id of ['A', 'B']) {
      ;(async () => {
        for (;;) {
          const r = await fetch(B + `/lane/claim?laneId=${id}&wait=1000`, { headers: H }).catch(() => null)
          if (!r) return
          if (r.status !== 200) continue
          const c = await r.json()
          await post('/lane/result', { laneId: id, entryId: c.entryId, result: { kind: 'limited', status: 429, ms: 5,
            raw: '{"error":{"type":"FreeUsageLimitError","message":"Rate limit exceeded. Please try again later."}}', data: null } })
        }
      })()
    }
    const res = await fetch(B + '/v1/chat/completions', { method: 'POST', headers: H,
      body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }) })
    await res.text()
    const h = await (await fetch(B + '/health')).json()
    const rl = h.rateLimits || {}
    return [
      { name: '/health counts rate limits', ok: (rl.count || 0) > 0, detail: JSON.stringify(rl) },
      { name: 'and records which model was refused', ok: rl.lastModel === 'big-pickle', detail: String(rl.lastModel) },
      { name: 'and it does NOT quarantine the model for it',
        ok: !(h.modelHealth?.['big-pickle']?.quarantined), detail: JSON.stringify(h.modelHealth?.['big-pickle']) },
    ]
  },

  // The converse must be preserved: an ordinary upstream fault is still a 502.
  async 'client: a genuine upstream fault is still 502' () {
    const { B, post } = await bootRelay(18925)
    await post('/lane/register', { laneId: 'A' })
    ;(async () => {
      for (;;) {
        const r = await fetch(B + '/lane/claim?laneId=A&wait=1000', { headers: H }).catch(() => null)
        if (!r || r.status !== 200) { if (!r) return; continue }
        const c = await r.json()
        await post('/lane/result', { laneId: 'A', entryId: c.entryId, result: { kind: 'provider_error', status: 400, ms: 5,
          raw: '{"error":{"message":"Missing required parameter: tools[4].type"}}', data: null } })
      }
    })()
    const res = await fetch(B + '/v1/chat/completions', { method: 'POST', headers: H,
      body: JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }] }) })
    const body = await res.json().catch(() => ({}))
    return [
      { name: 'a 400 upstream rejection is NOT reported as 429', ok: res.status === 502, detail: 'status ' + res.status },
      { name: 'and it keeps the upstream detail', ok: /tools\[4\]/.test(String(body?.error?.message || '')), detail: String(body?.error?.message).slice(0, 90) },
    ]
  },

  // classification: prose that quotes the user must not retire a lane.
  async 'classify: echoed text cannot exhaust a lane' () {
    const { classifyUpstream: c } = await imp('fingerprint.js')
    const rows = [
      ['200 answer mentioning rate limit', 200, 'how do I raise my rate limit?', 'ok'],
      ['400 echoing the prompt', 400, '{"error":{"message":"invalid prompt: how do I raise my rate limit?"}}', 'provider_error'],
      ['403 gate that mentions rate limits', 403, '{"error":{"type":"FreeTierError","message":"rate limits apply"}}', 'gate'],
      ['real 429', 429, '', 'limited'],
      ['429 whose body says the endpoint is down', 429, '{"error":{"type":"server_error","message":"Error from provider (Console): Upstream request failed: Endpoint is unavailable."}}', 'provider_error'],
      ['429 with a structured quota marker is still a limit', 429, '{"error":{"type":"FreeUsageLimitError","message":"Upstream request failed"}}', 'limited'],
      ['structured limit marker', 400, '{"error":{"type":"FreeUsageLimitError"}}', 'limited'],
      ['500 that says rate limit in prose', 500, 'upstream rate limit reached', 'limited'],
      ['plain 500', 500, 'boom', 'transport'],
      ['410 withdrawn by the provider', 410, '{"type":"error","error":{"type":"ModelDeprecated","message":"Model x has been deprecated."}}', 'gone'],
      ['404 not found', 404, '{"error":{"message":"model_not_found"}}', 'gone'],
      ['a 429 carrying a deprecation notice is still a withdrawal, not a rate limit', 429, '{"error":{"type":"ModelDeprecated"}}', 'gone'],
    ]
    return rows.map(([name, s, b, want]) => ({ name, ok: c(s, b).kind === want, detail: `got ${c(s, b).kind}, want ${want}` }))
  },

  // responses dialect: ids invented for id-less calls must pair with their outputs.
  async 'responses: anonymous call ids pair with their outputs' () {
    const { buildForTest } = await imp('lane.js')
    const body = buildForTest({ model: 'muse-spark-1.3-contributor-free', messages: [
      { role: 'user', content: 'go' },
      { role: 'assistant', content: null, tool_calls: [{ function: { name: 'read', arguments: '{}' } }, { function: { name: 'grep', arguments: '{}' } }] },
      { role: 'tool', content: 'one' }, { role: 'tool', content: 'two' },
    ] })
    const calls = body.input.filter((i) => i.type === 'function_call').map((i) => i.call_id)
    const outs = body.input.filter((i) => i.type === 'function_call_output').map((i) => i.call_id)
    return [
      { name: 'every output answers a call that exists', ok: outs.length === 2 && outs.every((o) => calls.includes(o)), detail: `calls=${calls} outputs=${outs}` },
      { name: 'calls are distinct', ok: new Set(calls).size === calls.length, detail: calls.join(',') },
    ]
  },

  // Thinking is forwarded on its own channel. Two things must hold, and the second
  // is the one that matters: reasoning must never be folded into the answer text,
  // because the fold is what a non-streaming client receives and what the relay
  // measures its streamed-text accounting against.
  async 'lane: thinking is forwarded, never folded into content' () {
    const { stop, port } = await mockUpstream((res) => {
      // Both field names the provider actually uses: reasoning_content (beam-free)
      // and reasoning (nemotron-3.5-lightning-free).
      res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: { reasoning_content: 'pondering' } }] })}\n\n`)
      res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: { reasoning: ' more' } }] })}\n\n`)
      res.write(chatChunk('the answer'))
      res.end('data: [DONE]\n\n')
    })
    process.env.ZEN_BASE = `https://127.0.0.1:${port}`
    const { callUpstream } = await imp('lane.js')
    const text = []
    const think = []
    const r = await callUpstream(
      { model: 'big-pickle', messages: [{ role: 'user', content: 'x' }], stream: true },
      { onDelta: (t) => text.push(t), onReasoning: (t) => think.push(t) })
    stop()
    return [
      { name: 'both provider field names reach onReasoning', ok: think.join('') === 'pondering more', detail: JSON.stringify(think.join('')) },
      { name: 'reasoning never reaches onDelta', ok: text.join('') === 'the answer', detail: JSON.stringify(text.join('')) },
      { name: 'the folded completion is the answer only', ok: r.data?.choices?.[0]?.message?.content === 'the answer', detail: JSON.stringify(r.data?.choices?.[0]?.message?.content) },
    ]
  },

  // #N the client-facing half: reasoning is written to the SSE stream as
  // delta.reasoning_content, and must NOT be counted as delivered answer text --
  // the finaliser uses that count to decide how much of the answer to send, so
  // counting thinking there would leave the client with no answer at all.
  async 'relay: thinking reaches the client without swallowing the answer' () {
    const { B, post } = await bootRelay(18909, { LANE_STALE_MS: '60000' })
    await post('/lane/register', { laneId: 'L1' })
    const client = fetch(B + '/v1/chat/completions', {
      method: 'POST', headers: H,
      body: JSON.stringify({ model: 'big-pickle', stream: true, messages: [{ role: 'user', content: 'hi' }] }),
    }).then((r) => r.text())
    const c1 = await fetch(B + '/lane/claim?laneId=L1&wait=3000', { headers: H }).then((r) => r.json())
    const d = await post('/lane/delta', { laneId: 'L1', entryId: c1.entryId, reasoning: 'thinking hard' })
    await post('/lane/result', {
      laneId: 'L1', entryId: c1.entryId,
      result: { kind: 'ok', status: 200, ms: 5, data: { choices: [{ index: 0, message: { role: 'assistant', content: 'the answer' }, finish_reason: 'stop' }] } },
    })
    const out = await Promise.race([client, sleep(4000).then(() => '')])
    const deltas = [...out.matchAll(/^data: (\{.*\})$/gm)].map((m) => JSON.parse(m[1])).map((j) => j.choices?.[0]?.delta || {})
    const thinking = deltas.map((x) => x.reasoning_content || '').join('')
    const answer = deltas.map((x) => x.content || '').join('')
    return [
      { name: 'the delta is accepted', ok: d.ok === true, detail: JSON.stringify(d) },
      { name: 'the client receives the thinking as reasoning_content', ok: thinking === 'thinking hard', detail: JSON.stringify(thinking) },
      { name: 'thinking is not delivered as answer content', ok: !answer.includes('thinking hard'), detail: JSON.stringify(answer) },
      { name: 'the answer still arrives in full', ok: answer.replace(/[^\x20-\x7e]/g, '').trim() === 'the answer', detail: JSON.stringify(answer) },
    ]
  },
}

// A TLS mock standing in for the upstream gateway.
async function mockUpstream (handler) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mock-'))
  execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', path.join(dir, 'k.pem'),
    '-out', path.join(dir, 'c.pem'), '-subj', '/CN=localhost', '-days', '1'], { stdio: 'ignore' })
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'
  const srv = https.createServer({ key: fs.readFileSync(path.join(dir, 'k.pem')), cert: fs.readFileSync(path.join(dir, 'c.pem')) }, (req, res) => {
    req.resume()
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    handler(res)
  })
  await new Promise((r) => srv.listen(0, '127.0.0.1', r))
  return { port: srv.address().port, stop: () => srv.close() }
}

// ---------------------------------------------------------------- runner
const arg = process.argv.indexOf('--scenario')
if (arg !== -1) {
  // child: run one scenario, print its results as JSON on the last line
  const name = process.argv[arg + 1]
  try {
    const results = await scenarios[name]()
    console.log('\n@@RESULT@@' + JSON.stringify(results))
  } catch (e) {
    console.log('\n@@RESULT@@' + JSON.stringify([{ name: 'scenario ran without throwing', ok: false, detail: String(e?.stack || e).slice(0, 300) }]))
  }
  process.exit(0)
}

console.log(`source under test: ${SRC}\n`)
// Children run from an empty directory: config.js reads baked-credentials.json from
// the working directory, and a project checkout has one (with a real relay token),
// which silently changed the token every scenario authenticates with.
const cleanCwd = fs.mkdtempSync(path.join(os.tmpdir(), 'regress-cwd-'))
let failures = 0, total = 0
for (const name of Object.keys(scenarios)) {
  if (process.env.ONLY && !name.includes(process.env.ONLY)) continue
  const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url), '--scenario', name],
    { env: { ...process.env, SRC_DIR: SRC }, cwd: cleanCwd, encoding: 'utf8', timeout: 120000 })
  const line = (child.stdout || '').split('\n').find((l) => l.startsWith('@@RESULT@@'))
  console.log(name)
  if (!line) {
    failures++; total++
    console.log(`  FAIL  scenario produced no result -- ${(child.stderr || child.stdout || '').slice(-200).replace(/\n/g, ' ')}`)
    continue
  }
  for (const r of JSON.parse(line.slice('@@RESULT@@'.length))) {
    total++
    if (!r.ok) failures++
    console.log(`  ${r.ok ? 'PASS' : 'FAIL'}  ${r.name}${r.ok ? '' : ' -- ' + r.detail}`)
  }
}
console.log(failures ? `\n${failures} of ${total} FAILED` : `\nALL ${total} REGRESSION CHECKS PASSED`)
process.exit(failures ? 1 : 0)
