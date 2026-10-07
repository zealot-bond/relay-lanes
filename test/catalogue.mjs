// End-to-end check of every catalogue entry through the ACTUAL request path:
// protocolFor() -> buildUpstreamBody() -> upstream, plus a streamed tool-bearing
// turn so the SSE fold and the tool-call plumbing are exercised too.
//
// This is deliberately not the same probe that discovered the models: that one
// hand-built a body, this one goes through the relay's own converter.
import { callUpstream, destroyKeepalive } from '/home/rhythm/Desktop/glm/src/lane.js'
import { MODELS, protocolFor, listModelsPayload } from '/home/rhythm/Desktop/glm/src/models.js'

const TOOLS = [
  { type: 'function', function: { name: 'read', description: 'Read a file', parameters: { type: 'object', properties: { file_path: { type: 'string' } }, required: ['file_path'] } } },
]

let failures = 0
const check = (name, cond, detail = '') => {
  if (cond) console.log(`    PASS  ${name}`)
  else { failures++; console.log(`    FAIL  ${name}${detail ? ' -- ' + detail : ''}`) }
}

console.log(`catalogue: ${MODELS.length} models`)
console.log(`served ids: ${listModelsPayload('test').data.length}\n`)

console.log('1. plain turn through the real converter')
const plain = []
for (const m of MODELS) {
  const r = await callUpstream({
    model: m.id,
    messages: [{ role: 'user', content: 'Reply with exactly one word: ok' }],
  })
  const text = String(r.data?.choices?.[0]?.message?.content ?? '').trim()
  const ok = r.kind === 'ok' && text.length > 0
  if (!ok) failures++
  plain.push(ok)
  console.log(`  ${ok ? 'PASS' : 'FAIL'} ${m.id.padEnd(34)} dialect=${protocolFor(m.id).padEnd(9)} ` +
    `${String(r.kind).padEnd(6)} ${String(r.ms).padStart(6)}ms ${JSON.stringify(text.slice(0, 24))}` +
    `${r.kind !== 'ok' ? '  ' + String(r.raw).slice(0, 90).replace(/\s+/g, ' ') : ''}`)
}

console.log('\n2. streamed turn (SSE fold + tool plumbing)')
for (const m of MODELS) {
  const chunks = []
  const toolDeltas = []
  const r = await callUpstream(
    { model: m.id, messages: [{ role: 'user', content: 'Name one colour, one word only.' }], tools: TOOLS },
    { onDelta: (t) => chunks.push(t), onToolDelta: (d) => toolDeltas.push(...d) },
  )
  const streamed = chunks.join('')
  const ok = r.kind === 'ok' && streamed.trim().length > 0
  if (!ok) failures++
  console.log(`  ${ok ? 'PASS' : 'FAIL'} ${m.id.padEnd(34)} ${String(r.kind).padEnd(6)} ` +
    `${String(r.ms).padStart(6)}ms chunks=${String(chunks.length).padStart(3)} ` +
    `chars=${String(streamed.length).padStart(4)} toolDeltas=${toolDeltas.length} ` +
    `${JSON.stringify(streamed.slice(0, 24))}`)
}

console.log('\n3. structural checks')
check('every model resolves via findModel', MODELS.every((m) =>
  listModelsPayload().data.some((d) => d.id === 'github/' + m.id)))
check('no duplicate ids', new Set(MODELS.map((m) => m.id)).size === MODELS.length)
check('responses dialect used only by muse-spark', MODELS.every((m) =>
  (protocolFor(m.id) === 'responses') === m.id.startsWith('muse-spark')))
check('no removed model still advertised',
  !MODELS.some((m) => ['mimo-v2.5-free', 'jev-1.13-free', 'jev-1.13', 'ling-3.0-flash-fin-free',
    'deepseek-v4-flash-free'].includes(m.id)),
  MODELS.map((m) => m.id).filter((id) => ['mimo-v2.5-free', 'jev-1.13-free', 'jev-1.13', 'ling-3.0-flash-fin-free', 'deepseek-v4-flash-free'].includes(id)).join(','))
check('public ids are namespaced and vendor-free',
  listModelsPayload().data.every((d) => d.id.startsWith('github/') && !/opencode|zen/i.test(d.id)))

console.log(`\n${failures ? failures + ' FAILURE(S)' : 'ALL CHECKS PASSED'} ` +
  `(${plain.filter(Boolean).length}/${plain.length} models answered)`)
destroyKeepalive()
process.exit(failures ? 1 : 0)