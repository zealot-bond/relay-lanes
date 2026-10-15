// Concurrency check through the LANE path.
//
// The runner pool exists to give every concurrent request its own egress bucket, so the
// property that matters is: N simultaneous requests all get answered, none is dropped,
// and none is refused to the client with a 429.
const BASE = process.env.BASE || 'http://127.0.0.1:26051'
const N = Number(process.env.N || 20)
const MODELS = (process.env.MODELS || 'big-pickle,cat-jam-free,space-bunny-free,muse-spark-1.3-contributor-free,mimo-v2.6-flash-free').split(',')
const TOOLS = [{ type: 'function', function: { name: 'bash', description: 'Run a bash command.', parameters: {
  type: 'object', properties: { command: { type: 'string' }, description: { type: 'string' } }, required: ['command', 'description'] } } }]

const one = async (model, i) => {
  const t = Date.now()
  // A tool prompt has to actually ask for the tool. "Say OK and call bash." reliably
// produces a bare {"command": "echo hello"} with no description -- correct model behaviour
// for an ambiguous ask, and it made this benchmark look like a relay bug when it was not.
const withTools = i % 3 === 0
  try {
    const res = await fetch(BASE + '/v1/chat/completions', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'github/' + model, stream: false, ...(withTools ? { tools: TOOLS } : {}),
        messages: [{ role: 'user', content: withTools ? 'List the files in /tmp using your shell tool.' : 'Reply with exactly: OK' }] }),
      signal: AbortSignal.timeout(180000),
    })
    const j = await res.json().catch(() => null)
    const m = j?.choices?.[0]?.message
    const call = m?.tool_calls?.[0]
    const ok = res.status === 200 && (String(m?.content || '').trim().length > 0 || Boolean(call))
    let argOk = true
    if (call) { try { const a = JSON.parse(call.function.arguments); argOk = Boolean(a.command && a.description) } catch { argOk = false } }
    return { ok: ok && argOk, status: res.status, ms: Date.now() - t, model, tools: withTools,
      why: j?.error ? String(j.error.message).slice(0, 70) : (argOk ? null : 'tool args incomplete') }
  } catch (e) { return { ok: false, status: 0, ms: Date.now() - t, model, tools: withTools, why: e.name } }
}

console.log(`BASE=${BASE}  N=${N} concurrent through the LANE path\n`)
const t0 = Date.now()
const rs = await Promise.all([...Array(N)].map((_, i) => one(MODELS[i % MODELS.length], i)))
const elapsed = Date.now() - t0

const ok = rs.filter((r) => r.ok)
const bad = rs.filter((r) => !r.ok)
console.log(`served      : ${ok.length}/${N}`)
console.log(`wall clock  : ${(elapsed / 1000).toFixed(1)}s`)
console.log(`client 429s : ${rs.filter((r) => r.status === 429).length}   <- must be 0`)
console.log(`client 5xx  : ${rs.filter((r) => r.status >= 500).length}`)
console.log(`p50 / p95   : ${[...ok.map((r) => r.ms)].sort((a, b) => a - b)[Math.floor(ok.length * 0.5)] ?? '-'}ms / ${[...ok.map((r) => r.ms)].sort((a, b) => a - b)[Math.floor(ok.length * 0.95)] ?? '-'}ms`)
console.log('\nby model:')
for (const m of MODELS) {
  const s = rs.filter((r) => r.model === m)
  const t = s.filter((r) => r.tools)
  console.log(`  ${m.padEnd(34)} ${s.filter((r) => r.ok).length}/${s.length}` + (t.length ? `  (tools ${t.filter((r) => r.ok).length}/${t.length})` : ''))
}
if (bad.length) {
  console.log('\nfailures:')
  for (const b of bad.slice(0, 8)) console.log(`  ${b.model} tools=${b.tools} status=${b.status} ${b.why || ''}`)
}
const h = await (await fetch(BASE + '/health')).json()
console.log(`\nrelay: served=${h.served} failed=${h.failed} retired=${h.retired} timeouts=${h.timeouts} lanes=${h.lanes}`)
process.exit(bad.length || rs.some((r) => r.status === 429) ? 1 : 0)