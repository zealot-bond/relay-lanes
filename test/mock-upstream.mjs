// A controllable stand-in for the upstream gateway.
//
// Used by test/adversarial.mjs. Lives in test/ rather than in src/ because it is a
// test fixture: the relay never imports it, and shipping it in the jar would put a
// listener for arbitrary responses inside the production payload.
//
// The relay and lane code are exercised for real: main.js + worker.js talk to THIS
// server over HTTPS, so every byte of the real transport, SSE fold, delta batching and
// client writer is in the path. Only the provider is replaced, which is what makes
// failures reproducible instead of intermittent.
//
// The client picks a scenario with a marker in the prompt: [[S:sparse_indices]]
import https from 'node:https'
import fs from 'node:fs'
import { execFileSync } from 'node:child_process'
import os from 'node:os'
import path from 'node:path'

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'up-'))
execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes',
  '-keyout', path.join(dir, 'k.pem'), '-out', path.join(dir, 'c.pem'),
  '-subj', '/CN=localhost', '-days', '1'], { stdio: 'ignore' })

export const seen = []   // every request body the relay sent upstream

const sse = (o) => `data: ${JSON.stringify(o)}\n\n`
const chunk = (delta, finish = null) => sse({ id: 'x', object: 'chat.completion.chunk',
  created: 1, model: 'm', choices: [{ index: 0, delta, finish_reason: finish }] })

// Each scenario writes a response. `res` is the raw ServerResponse, so a scenario can
// send garbage, stall, reset the socket, or answer with a non-SSE body.
const scenarios = {
  // --- well-behaved baselines
  longtext: (res) => {
    const body = 'A hash table stores keys in an array of buckets, computed from each key, so that lookups need only one array access plus a short chain walk. Collisions happen whenever two keys land in the same bucket, which is why the table resizes when the buckets get crowded.'
    for (let i = 0; i < body.length; i += 24) res.write(chunk({ content: body.slice(i, i + 24) }))
    res.end(chunk({}, 'stop') + 'data: [DONE]\n\n')
  },
  plain: (res) => { for (const w of ['Hel', 'lo ', 'world']) res.write(chunk({ content: w })); res.end(sse({ choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] }) + 'data: [DONE]\n\n') },

  // One call, arguments dribbled out in tiny pieces (how real streams look).
  tool_small_deltas: (res) => {
    const args = '{"command":"ls -la /tmp","description":"List files in /tmp"}'
    res.write(chunk({ tool_calls: [{ index: 0, id: 'call_a', type: 'function', function: { name: 'bash', arguments: '' } }] }))
    for (let i = 0; i < args.length; i += 7) res.write(chunk({ tool_calls: [{ index: 0, function: { arguments: args.slice(i, i + 7) } }] }))
    res.end(chunk({}, 'tool_calls') + 'data: [DONE]\n\n')
  },

  // Two calls announced together and then interleaved, with a sparse index (2 first).
  // A real hazard: if the relay numbers calls by arrival order, the arguments swap.
  //
  // Arguments are emitted as FRAGMENTS of one JSON document, the way a real provider
  // streams them. Sending a complete object per delta is not a protocol violation on
  // the relay's part -- it must concatenate -- but it makes the mock test the wrong
  // thing, so this splits each document into pieces.
  sparse_indices: (res) => {
    const docA = '{"command":"echo A","description":"run A"}'
    const docB = '{"pattern":"B","path":"/tmp"}'
    res.write(chunk({ tool_calls: [
      { index: 2, id: 'call_B', type: 'function', function: { name: 'grep', arguments: '' } },
      { index: 0, id: 'call_A', type: 'function', function: { name: 'bash', arguments: '' } },
    ] }))
    for (let i = 0; i < 30; i++) {
      res.write(chunk({ tool_calls: [
        { index: 2, function: { arguments: (docB + ' ').slice(i * 4, i * 4 + 4) } },
        { index: 0, function: { arguments: (docA + ' ').slice(i * 5, i * 5 + 5) } },
      ] }))
    }
    res.end(chunk({}, 'tool_calls') + 'data: [DONE]\n\n')
  },

  // A call whose arguments are large -- the case that used to overflow the SSE writer.
  tool_big_args: (res) => {
    const big = 'x'.repeat(120000)
    const args = JSON.stringify({ command: `echo ${big}`, description: 'big' })
    res.write(chunk({ tool_calls: [{ index: 0, id: 'c', type: 'function', function: { name: 'bash', arguments: args } }] }))
    res.end(chunk({}, 'tool_calls') + 'data: [DONE]\n\n')
  },

  // Text and a tool call in the same turn: the client must get both, not one.
  text_and_tool: (res) => {
    res.write(chunk({ content: 'Let me check. ' }))
    res.write(chunk({ tool_calls: [{ index: 0, id: 't', type: 'function', function: { name: 'bash', arguments: '{"command":"date","description":"Show date"}' } }] }))
    res.end(chunk({}, 'tool_calls') + 'data: [DONE]\n\n')
  },

  // Reasoning models emit this before the answer; it must not be shown as content.
  reasoning_then_text: (res) => {
    res.write(sse({ type: 'response.reasoning_summary_text.delta', delta: 'thinking hard' }))
    res.write(chunk({ content: 'The answer is 4. ' }))
    res.write(sse({ type: 'response.reasoning_summary_text.delta', delta: 'done thinking' }))
    res.end(chunk({ content: 'That is all.' }, 'stop') + 'data: [DONE]\n\n')
  },

  // Everything in a single write: no boundary the reader can rely on.
  one_shot: (res) => res.end(chunk({ content: 'all at once, ' }) + chunk({ content: 'no newlines between' }, 'stop') + 'data: [DONE]\n\n'),

  // CRLF line endings, which SSE permits.
  crlf: (res) => res.end(
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: 'crlf ' } }] })}\r\n\r\n` +
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: 'works' }, finish_reason: 'stop' }] })}\r\n\r\n` +
    'data: [DONE]\r\n\r\n'),

  // Multi-byte characters arriving one byte per write: the classic split-UTF-8 trap.
  utf8_split: (res) => {
    const b = Buffer.from(chunk({ content: '日本語 😀 café — ✓' }) + 'data: [DONE]\n\n')
    let i = 0
    const tick = () => {
      if (i >= b.length) return res.end()
      res.write(b.subarray(i, i + 1)); i++
      setImmediate(tick)
    }
    tick()
  },

  // A keepalive comment and a non-JSON data line before the real content.
  keepalive_then_data: (res) => {
    res.write(': ping\n\n')
    res.write('data: not-json\n\n')
    res.write(chunk({ content: 'survived ' }))
    res.end(chunk({ content: 'the noise' }, 'stop') + 'data: [DONE]\n\n')
  },

  // --- misbehaving upstream: each must become a clean, typed failure
  die_midstream: (res) => { res.write(chunk({ content: 'half an ans' })); setTimeout(() => res.socket.destroy(), 30) },
  never_reply: (res) => { /* hold the socket open, send nothing */ },
  http_500: (res) => { res.writeHead(500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: { message: 'provider exploded' } })) },
  http_429_real: (res) => { res.writeHead(429, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: { type: 'rate_limit_error', message: 'slow down' } })) },
  http_429_outage: (res) => { res.writeHead(429, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: { type: 'server_error', message: 'Upstream request failed: Endpoint is unavailable.' } })) },
  http_403_gate: (res) => { res.writeHead(403, { 'content-type': 'application/json' }); res.end(JSON.stringify({ type: 'error', error: { type: 'FreeTierError', message: "OpenCode's free tier can only be used from within OpenCode." } })) },
  http_400: (res) => { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: { message: 'Missing required parameter: tools[4].type' } })) },
  // The provider withdrew the model. exo-free and fledge-alpha-free both answered
  // this for eight days before being taken out of the catalogue (2026-10-15).
  http_410_gone: (res) => { res.writeHead(410, { 'content-type': 'application/json' }); res.end(JSON.stringify({ type: 'error', error: { type: 'ModelDeprecated', message: 'Model demo-gone has been deprecated.' } })) },
  // This IP's bucket is spent. A client behind a rate-limited relay sees this as the whole
  // catalogue going quiet at once, and the only real remedy is a different egress IP.
  free_usage_limit: (res) => { res.writeHead(429, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: { type: 'FreeUsageLimitError', message: 'Rate limit exceeded. Please try again later.' } })) },
  empty_200: (res) => { res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ choices: [{ index: 0, message: { role: 'assistant', content: '' }, finish_reason: 'stop' }] })) },
  error_after_text: (res) => { res.write(chunk({ content: 'committed text ' })); setTimeout(() => { res.write(sse({ error: { message: 'died mid answer' } })); res.end() }, 20) },
  json_not_sse: (res) => { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end(JSON.stringify({ choices: [{ index: 0, message: { role: 'assistant', content: 'json body' }, finish_reason: 'stop' }] })) },
  stream_truncated: (res) => { res.write(chunk({ content: 'starts fine but never finishes' })); res.end() },
  garbage: (res) => { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write('this is not sse at all\n'); res.end('\x00\x01\x02binary junk\n') },
}

const handler = https.createServer({ key: fs.readFileSync(path.join(dir, 'k.pem')), cert: fs.readFileSync(path.join(dir, 'c.pem')) }, (req, res) => {
  let body = ''
  req.on('data', (c) => { body += c })
  req.on('end', () => {
    let parsed = null
    try { parsed = JSON.parse(body) } catch { /* recorded anyway */ }
    const text = body
    const m = /\[\[S:([a-z0-9_]+)\]\]/.exec(text)
    // No marker -> `longtext`, a paragraph. A prober treats "under 20 characters" as an
    // empty answer, so the default fixture must be long enough to pass that threshold
    // honestly. Tests that want the short exact-match fixture ask for [[S:plain]].
    // MOCK_DEFAULT lets a prober be pointed at a fixture that answers in a chosen mode.
    const name = m ? m[1] : (process.env.MOCK_DEFAULT || 'longtext')
    seen.push({ scenario: name, path: req.url, body: parsed, raw: body })
    const fn = scenarios[name]
    if (!fn) { res.writeHead(400); return res.end(JSON.stringify({ error: { message: 'unknown scenario ' + name } })) }
    try { fn(res) } catch (e) { try { res.destroy() } catch {} }
  })
})

export const start = () => new Promise((r) => handler.listen(0, '127.0.0.1', () => r(handler.address().port)))
export const stop = () => handler.close()
export const names = Object.keys(scenarios)

// Run directly as a server: bind and print the URL so a shell script can pick it up.
if (process.argv[1] && process.argv[1].endsWith('mock-upstream.mjs')) {
  const port = await start()
  console.log(`http://127.0.0.1:${port}`)
}