// Unit tests for the tool-call merge and SSE fold, run without a relay.
//
//   node test/units.mjs
//
// These cover the exact wire shapes that produced the reported failure
// ("missing required property file_path / old_string / new_string") and the
// duplication/interleaving symptoms, so a regression fails here in milliseconds
// instead of needing a live runner.

import { parseForTest, buildForTest } from '../src/lane.js'

let failures = 0
const check = (name, cond, detail = '') => {
  if (cond) console.log(`  PASS  ${name}`)
  else { failures++; console.log(`  FAIL  ${name}${detail ? ` -- ${detail}` : ''}`) }
}

// The merger is imported from production, not re-implemented here. A byte-identical
// copy in the test file cannot fail when the real code regresses -- which is exactly
// how a tool-argument aliasing bug stayed live while this suite stayed green.
import { appendName, mergeToolDeltas } from '../src/toolmerge.js'


/** Fold streamed fragments the way a strict harness does: one entry per index. */
const assemble = (fragBatches) => {
  const byIndex = new Map()
  for (const batch of fragBatches) {
    for (const tc of batch) {
      const i = tc.index ?? 0
      const cur = byIndex.get(i) || { id: undefined, name: '', args: '' }
      if (tc.id) cur.id = tc.id
      if (tc.function?.name) cur.name += tc.function.name
      if (tc.function?.arguments) cur.args += tc.function.arguments
      byIndex.set(i, cur)
    }
  }
  return [...byIndex.entries()].map(([i, v]) => ({ index: i, ...v }))
}

console.log('merge: identity and arguments in separate windows')
{
  const args = JSON.stringify({ file_path: '/tmp/a', old_string: 'x', new_string: 'y' })
  const acc = new Map()
  const b1 = mergeToolDeltas([{ index: 0, id: 'call_1', type: 'function', function: { name: 'edit', arguments: '' } }], acc)
  const b2 = mergeToolDeltas([{ index: 0, function: { arguments: args.slice(0, 15) } }], acc)
  const b3 = mergeToolDeltas([{ index: 0, function: { arguments: args.slice(15) } }], acc)
  const calls = assemble([b1, b2, b3])
  check('one call', calls.length === 1, JSON.stringify(calls))
  check('id survives across batches', calls[0]?.id === 'call_1', String(calls[0]?.id))
  check('name survives across batches', calls[0]?.name === 'edit', JSON.stringify(calls[0]?.name))
  let p = null
  try { p = JSON.parse(calls[0].args) } catch { /* reported */ }
  check('arguments reassemble to valid JSON', p !== null, JSON.stringify(calls[0]?.args))
  check('all three required properties present',
    p?.file_path === '/tmp/a' && p?.old_string === 'x' && p?.new_string === 'y', JSON.stringify(p))
}

console.log('\nmerge: a name repeated on every fragment is not doubled')
{
  const acc = new Map()
  const bs = [
    mergeToolDeltas([{ index: 0, id: 'c', function: { name: 'edit', arguments: '{"a":' } }], acc),
    mergeToolDeltas([{ index: 0, function: { name: 'edit', arguments: '1}' } }], acc),
    mergeToolDeltas([{ index: 0, function: { name: 'edit', arguments: '' } }], acc),
  ]
  const calls = assemble(bs)
  check('name is edit, not editedit', calls[0]?.name === 'edit', JSON.stringify(calls[0]?.name))
  check('arguments intact', calls[0]?.args === '{"a":1}', JSON.stringify(calls[0]?.args))
}

console.log('\nmerge: a name split across fragments is joined')
{
  const acc = new Map()
  const bs = [
    mergeToolDeltas([{ index: 0, id: 'c', function: { name: 'ed', arguments: '' } }], acc),
    mergeToolDeltas([{ index: 0, function: { name: 'it', arguments: '{}' } }], acc),
  ]
  const calls = assemble(bs)
  // The client only ever receives the first fragment of the name by design, so
  // assert the accumulator produced the whole name for the folded result.
  check('accumulated name is edit', acc.get(0).function.name === 'edit', JSON.stringify(acc.get(0).function.name))
  check('client-side name is the first fragment', calls[0]?.name === 'ed', JSON.stringify(calls[0]?.name))
}

console.log('\nmerge: parallel calls keep their own indices')
{
  const acc = new Map()
  const bs = [
    mergeToolDeltas([
      { index: 0, id: 'a', function: { name: 'edit', arguments: '{"f":1' } },
      { index: 1, id: 'b', function: { name: 'read', arguments: '{"g":2' } },
    ], acc),
    mergeToolDeltas([
      { index: 1, function: { arguments: ',"h":3}' } },
      { index: 0, function: { arguments: ',"i":4}' } },
    ], acc),
  ]
  const calls = assemble(bs)
  check('two calls', calls.length === 2, JSON.stringify(calls))
  check('index 0 intact', calls[0]?.index === 0 && calls[0]?.name === 'edit' && calls[0]?.args === '{"f":1,"i":4}',
    JSON.stringify(calls[0]))
  check('index 1 intact', calls[1]?.index === 1 && calls[1]?.name === 'read' && calls[1]?.args === '{"g":2,"h":3}',
    JSON.stringify(calls[1]))
  check('both calls parse as JSON',
    (() => { try { JSON.parse(calls[0].args); JSON.parse(calls[1].args); return true } catch { return false } })())
}

console.log('\nmerge: interleaved fragments never merge into one entry')
{
  const acc = new Map()
  const bs = [mergeToolDeltas([
    { index: 0, id: 'a', function: { name: 'edit', arguments: '{"file_path":"' } },
    { index: 1, id: 'b', function: { name: 'write', arguments: '{"path":"' } },
    { index: 0, function: { arguments: '/x"}' } },
    { index: 1, function: { arguments: '/y"}' } },
  ], acc)]
  const calls = assemble(bs)
  check('still two calls', calls.length === 2, JSON.stringify(calls))
  check('call 0 got only its own args', calls[0]?.args === '{"file_path":"/x"}', JSON.stringify(calls[0]?.args))
  check('call 1 got only its own args', calls[1]?.args === '{"path":"/y"}', JSON.stringify(calls[1]?.args))
}

// ---------------------------------------------------------------- SSE folding
console.log('\nparseUpstream: chat dialect tool call')
{
  const args = JSON.stringify({ file_path: '/p', old_string: 'a', new_string: 'b' })
  const raw = [
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: 'call_z', type: 'function', function: { name: 'edit', arguments: '' } }] } }] })}`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: args.slice(0, 9) } }] } }] })}`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: args.slice(9) } }] } }] })}`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] })}`,
    'data: [DONE]',
  ].join('\n')
  const out = parseForTest(raw, 'big-pickle')
  const tc = out.choices[0].message.tool_calls
  check('one call', Array.isArray(tc) && tc.length === 1, JSON.stringify(tc))
  check('id present', tc?.[0]?.id === 'call_z', String(tc?.[0]?.id))
  check('name present', tc?.[0]?.function?.name === 'edit', JSON.stringify(tc?.[0]?.function?.name))
  let p = null
  try { p = JSON.parse(tc[0].function.arguments) } catch { /* reported */ }
  check('arguments valid', p !== null, JSON.stringify(tc?.[0]?.function?.arguments))
  check('file_path present', p?.file_path === '/p', JSON.stringify(p))
  check('finish_reason tool_calls', out.choices[0].finish_reason === 'tool_calls', out.choices[0].finish_reason)
}

console.log('\nparseUpstream: responses dialect, parallel calls by output_index')
{
  const raw = [
    `data: ${JSON.stringify({ type: 'response.output_item.added', output_index: 0, item: { type: 'function_call', id: 'fc_0', name: 'edit' } })}`,
    `data: ${JSON.stringify({ type: 'response.output_item.added', output_index: 1, item: { type: 'function_call', id: 'fc_1', name: 'read' } })}`,
    // Arguments arrive interleaved, and the item_id is the only reliable key here.
    `data: ${JSON.stringify({ type: 'response.function_call_arguments.delta', output_index: 1, item_id: 'fc_1', delta: '{"path":"' })}`,
    `data: ${JSON.stringify({ type: 'response.function_call_arguments.delta', output_index: 0, item_id: 'fc_0', delta: '{"file_path":"' })}`,
    `data: ${JSON.stringify({ type: 'response.function_call_arguments.delta', output_index: 1, item_id: 'fc_1', delta: '/y"}' })}`,
    `data: ${JSON.stringify({ type: 'response.function_call_arguments.delta', output_index: 0, item_id: 'fc_0', delta: '/x"}' })}`,
    `data: ${JSON.stringify({ type: 'response.completed', response: { output_text: '' } })}`,
  ].join('\n')
  const out = parseForTest(raw, 'muse-spark-1.2-contributor-free')
  const tc = out.choices[0].message.tool_calls
  check('two calls', Array.isArray(tc) && tc.length === 2, JSON.stringify(tc))
  check('call 0 is edit with its own args',
    tc?.[0]?.function?.name === 'edit' && tc?.[0]?.function?.arguments === '{"file_path":"/x"}',
    JSON.stringify(tc?.[0]))
  check('call 1 is read with its own args',
    tc?.[1]?.function?.name === 'read' && tc?.[1]?.function?.arguments === '{"path":"/y"}',
    JSON.stringify(tc?.[1]))
  check('ids preserved', tc?.[0]?.id === 'fc_0' && tc?.[1]?.id === 'fc_1', `${tc?.[0]?.id},${tc?.[1]?.id}`)
  check('finish_reason tool_calls', out.choices[0].finish_reason === 'tool_calls', out.choices[0].finish_reason)
}

console.log('\nparseUpstream: text is never double counted')
{
  const raw = [
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: 'Hello' } }] })}`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: ' world' } }] })}`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}`,
    'data: [DONE]',
  ].join('\n')
  const seen = []
  const out = parseForTest(raw, 'big-pickle', (t) => seen.push(t))
  check('folded text correct', out.choices[0].message.content === 'Hello world', JSON.stringify(out.choices[0].message.content))
  check('streamed fragments correct', seen.join('') === 'Hello world', JSON.stringify(seen.join('')))
  check('no tool calls', !out.choices[0].message.tool_calls)
}

console.log('\nparseUpstream: reasoning is not emitted as content')
{
  const raw = [
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { reasoning: 'thinking hard' } }] })}`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: 'answer' } }] })}`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}`,
    'data: [DONE]',
  ].join('\n')
  const seen = []
  const out = parseForTest(raw, 'nemotron-3-ultra-free', (t) => seen.push(t))
  check('only content is forwarded', seen.join('') === 'answer', JSON.stringify(seen.join('')))
  check('folded content is the answer', out.choices[0].message.content === 'answer')
}

console.log('\nparseUpstream: text and a tool call in one turn')
{
  const raw = [
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: 'Reading.' } }] })}`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: 'c1', type: 'function', function: { name: 'read', arguments: '{"p":1}' } }] } }] })}`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] })}`,
    'data: [DONE]',
  ].join('\n')
  const out = parseUpstreamSafe(raw)
  check('text kept', out.choices[0].message.content === 'Reading.')
  check('call kept', out.choices[0].message.tool_calls?.[0]?.function?.name === 'read')
  check('finish_reason tool_calls', out.choices[0].finish_reason === 'tool_calls')
}
function parseUpstreamSafe (raw) { return parseForTest(raw, 'big-pickle') }

// ------------------------------------------------------- dialect conversion
console.log('\nbuildUpstreamBody: responses dialect shapes')
{
  const msgs = [
    { role: 'system', content: 'be terse' },
    { role: 'user', content: 'hi' },
    { role: 'assistant', content: '', tool_calls: [{ id: 'c1', type: 'function', function: { name: 'edit', arguments: '{"file_path":"/a"}' } }] },
    { role: 'tool', tool_call_id: 'c1', content: 'ok' },
    { role: 'user', content: [{ type: 'text', text: 'again' }, { type: 'image_url', image_url: { url: 'http://x/i.png' } }] },
  ]
  const body = buildForTest({ model: 'muse-spark-1.2-contributor-free', messages: msgs, maxTokens: 100, tools: [{ type: 'function', function: { name: 'edit', parameters: { type: 'object', properties: {} } } }] })
  check('input is an array', Array.isArray(body.input) && body.input.length > 0)
  check('no role:tool message survives', !body.input.some((i) => i.role === 'tool'))
  check('tool result is function_call_output', body.input.some((i) => i.type === 'function_call_output' && i.call_id === 'c1'))
  check('assistant tool call is function_call', body.input.some((i) => i.type === 'function_call' && i.name === 'edit' && i.call_id === 'c1'))
  check('system folded in as a system message', body.input.some((i) => i.role === 'system'))
  check('image part preserved as input_image',
    body.input.some((i) => Array.isArray(i.content) && i.content.some((p) => p.type === 'input_image')))
  // Client tools are forwarded alongside the gateway's four, which must stay present
  // exactly as spelled. (An earlier version of this test asserted the client tool was
  // DROPPED, encoding a conclusion drawn from stale runner code.)
  check('client tool forwarded and all four gateway tools retained',
    ['bash', 'glob', 'grep', 'read', 'edit'].every((n) => body.tools.some((t) => t.name === n)),
    JSON.stringify(body.tools.map((t) => t.name)))
  check('stream is forced true', body.stream === true)
  check('output budget raised to the reasoning floor', body.max_output_tokens === 4096, String(body.max_output_tokens))
}

console.log('\nbuildUpstreamBody: chat dialect keeps tool specs unioned')
{
  const body = buildForTest({ model: 'big-pickle', messages: [{ role: 'user', content: 'hi' }], tools: [{ type: 'function', function: { name: 'edit', parameters: { type: 'object', properties: {} } } }] })
  const names = body.tools.map((t) => t.function?.name)
  check('client tool present', names.includes('edit'), JSON.stringify(names))
  check('fingerprint tools retained', ['bash', 'glob', 'grep', 'read'].every((n) => names.includes(n)), JSON.stringify(names))
  check('fingerprint tools not duplicated', new Set(names).size === names.length, JSON.stringify(names))
}

console.log('\ntoResponsesInput: empty conversation is never an empty input')
{
  const body = buildForTest({ model: 'muse-spark-1.2-contributor-free', messages: [{ role: 'assistant', content: '' }] })
  check('input is non-empty', Array.isArray(body.input) && body.input.length >= 1, JSON.stringify(body.input))
}

console.log('\ntoResponsesInput: parallel id-less calls get distinct call ids')
{
  const body = buildForTest({
    model: 'muse-spark-1.2-contributor-free',
    messages: [
      { role: 'user', content: 'do two things' },
      {
        role: 'assistant', content: '',
        tool_calls: [
          { type: 'function', function: { name: 'edit', arguments: '{}' } },
          { type: 'function', function: { name: 'read', arguments: '{}' } },
        ],
      },
    ],
  })
  const ids = body.input.filter((i) => i.type === 'function_call').map((i) => i.call_id)
  check('two function_call items', ids.length === 2, JSON.stringify(body.input))
  check('call ids are distinct', new Set(ids).size === 2, JSON.stringify(ids))
}

console.log(`\n${failures === 0 ? 'ALL UNIT CHECKS PASSED' : `${failures} UNIT CHECK(S) FAILED`}\n`)
process.exit(failures === 0 ? 0 : 1)
