// Probe every advertised model through the relay and report what actually answers.
//
//   node scripts/probe-catalogue.mjs                       # against the live panel
//   BASE=http://127.0.0.1:26001 node scripts/probe-catalogue.mjs
//   N=8 M=5 node scripts/probe-catalogue.mjs
//
// Why this exists: the catalogue rots silently. exo-free and fledge-alpha-free were
// both in the "reliable" list on 2026-10-07 and answered 410 ModelDeprecated by
// 2026-10-15; nemotron-3-ultra-free was de-listed and only visible as such. Nothing
// alerted. Run this after any gap of a few days, and BEFORE trusting a model list.
//
// Two things it deliberately does NOT do:
//   * it does not trust the gateway's published model list as a health signal --
//     mimo-v2.5-free was listed for days while answering 410 on every call;
//   * it does not treat HTTP 200 as success. A provider refusal arrives as a 200
//     stream carrying an error frame, and an empty answer is served as 200 too.
//     A model passes only if it produced text, or a tool call whose arguments parse
//     and carry every required property.
import { execFileSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const BASE = process.env.BASE || 'http://160.187.210.9:25566'
const N = Number(process.env.N || 6)
const M = Number(process.env.M || 4)
const ONLY = process.env.MODELS || ''

const BASH = {
  type: 'object',
  properties: {
    command: { type: 'string', description: 'The command to execute' },
    description: { type: 'string', description: 'Clear, concise description of what this does.' },
  },
  required: ['command', 'description'],
}
const TOOLS = [{ type: 'function', function: { name: 'bash', description: 'Executes a bash command.', parameters: BASH } }]
const PLAIN = [
  'In about 100 words, explain how a hash table handles collisions. Plain prose.',
  'In about 100 words, explain what a mutex is. Plain prose.',
  'In about 100 words, explain why the sky is blue. Plain prose.',
]
const TOOLP = ['List the files in /tmp using your shell tool.', 'Check which node version is installed.', 'Show the current git branch.']

// Read the stream AS IT ARRIVES. Awaiting res.text() and parsing afterwards timestamps
// every frame at the moment the stream ended, which reports ttft == total and a
// nonsense tokens/sec.
async function sse (body, timeoutMs = 300000) {
  const t0 = Date.now()
  let ttft = null, chars = 0, lastAt = null, err = null, status = 0, raw = ''
  const acc = new Map()
  try {
    const res = await fetch(`${BASE}/v1/chat/completions`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...body, stream: true }), signal: AbortSignal.timeout(timeoutMs),
    })
    status = res.status
    const reader = res.body.getReader()
    const dec = new TextDecoder()
    let buf = ''
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      const s = dec.decode(value, { stream: true })
      raw += s; buf += s
      let k
      while ((k = buf.indexOf('\n\n')) !== -1) {
        const frame = buf.slice(0, k); buf = buf.slice(k + 2)
        const line = frame.split('\n').find((x) => x.startsWith('data:'))
        if (!line) continue
        const p = line.slice(5).trim()
        if (!p || p === '[DONE]') continue
        let e; try { e = JSON.parse(p) } catch { continue }
        if (e.error) { err = String(e.error.message).slice(0, 90); continue }
        const d = e.choices?.[0]?.delta || {}
        if (typeof d.content === 'string' && d.content) { if (ttft === null) ttft = Date.now() - t0; chars += d.content.length; lastAt = Date.now() - t0 }
        for (const tc of d.tool_calls || []) {
          const i = tc.index ?? 0
          const c = acc.get(i) || { name: '', args: '' }
          if (tc.function?.name) c.name += tc.function.name
          if (tc.function?.arguments) c.args += tc.function.arguments
          acc.set(i, c)
        }
      }
    }
    if (status !== 200 && !err) err = `HTTP ${status}: ${raw.slice(0, 90).replace(/\s+/g, ' ')}`
  } catch (e) { err = e.name }
  return { total: Date.now() - t0, ttft, chars, lastAt, err, status, calls: [...acc.values()] }
}

const plainOne = async (model, i) => {
  const r = await sse({ model, messages: [{ role: 'user', content: PLAIN[i % PLAIN.length] }] })
  const genMs = (r.lastAt ?? r.total) - (r.ttft ?? 0)
  const ok = !r.err && r.chars > 20
  return { ok, why: r.err || (r.chars ? null : 'empty answer'), ttft: r.ttft, total: r.total,
    tps: ok && genMs > 0 ? (r.chars / 4) / (genMs / 1000) : null }
}
const toolOne = async (model, i) => {
  const r = await sse({ model, messages: [{ role: 'user', content: TOOLP[i % TOOLP.length] }], tools: TOOLS })
  if (r.err) return { ok: false, why: r.err }
  const c = r.calls[0]
  if (!c) return { ok: false, why: 'no tool call (text only)' }
  let a; try { a = JSON.parse(c.args) } catch { return { ok: false, why: `unparseable args ${JSON.stringify(c.args).slice(0, 44)}` } }
  const miss = ['command', 'description'].filter((k) => !a[k])
  if (miss.length) return { ok: false, why: `missing ${miss.join('+')}` }
  if (!c.name) return { ok: false, why: 'call has no name' }
  return { ok: true }
}
const pct = (a, p) => { const s = a.filter((x) => x != null).sort((x, y) => x - y); return s.length ? s[Math.min(s.length - 1, Math.floor(p / 100 * s.length))] : null }

const health = await fetch(`${BASE}/health`, { signal: AbortSignal.timeout(15000) })
  .then((r) => (r.ok ? r.json() : null))
  .catch(() => null)
if (!health) {
  console.error(`no relay answering at ${BASE}`)
  console.error('start one, or point BASE at the panel. To probe without a panel:')
  console.error('  bash /tmp/opencode/boot-mock.sh && BASE=http://127.0.0.1:26001 node scripts/probe-catalogue.mjs')
  process.exit(1)
}
const advertised = (await (await fetch(`${BASE}/v1/models`)).json()).data.map((m) => m.id)
const models = ONLY ? ONLY.split(',') : advertised

console.log(`relay ${health.build} | lanes ${health.lanes}/${health.maxLanes} | ${advertised.length} advertised`)
console.log(`probing ${models.length} model(s), ${N} plain + ${M} tool calls each\n`)
console.log('model                              plain   tools   total p50   ttft p50   verdict')

const dead = []
const rows = []
for (const model of models) {
  const [plain, tools] = await Promise.all([
    Promise.all([...Array(N)].map((_, i) => plainOne(model, i))),
    Promise.all([...Array(M)].map((_, i) => toolOne(model, i))),
  ])
  const pOk = plain.filter((r) => r.ok), tOk = tools.filter((r) => r.ok)
  const why = [...new Set([...plain.filter((r) => !r.ok).map((r) => r.why), ...tools.filter((r) => !r.ok).map((r) => r.why)])]
  const dead_ = pOk.length === 0 && tOk.length === 0
  if (dead_) dead.push(model)
  const verdict = dead_ ? 'DEAD -- remove from the catalogue'
    : pOk.length + tOk.length === N + M ? 'reliable'
    : pOk.length === 0 || tOk.length === 0 ? 'partial -- one mode never works'
    : 'usable but flaky'
  const row = { model, verdict, why }
  rows.push(row)
  console.log(`${model.replace('github/', '').padEnd(34)} ${String(pOk.length) + '/' + N}  ${String(tOk.length) + '/' + M}   ` +
    `${String(pct(pOk.map((r) => r.total), 50) ?? '-').padStart(8)}ms  ${String(pct(pOk.map((r) => r.ttft), 50) ?? '-').padStart(8)}ms   ${verdict}`)
  for (const w of why.slice(0, 2)) console.log(`      ${w}`)

  if (dead_) console.log(`      ^ every attempt failed ${N + M} times`)
}

const h2 = await (await fetch(`${BASE}/health`)).json()
console.log(`\nrelay after: served +${h2.served - health.served} failed +${h2.failed - health.failed} retired +${h2.retired - health.retired} lanes ${h2.lanes} laneCode ${JSON.stringify(h2.laneCode)}`)
if (dead.length) {
  console.log(`\nREMOVE from src/models.js: ${dead.map((m) => m.replace('github/', '')).join(', ')}`)
  console.log('(and record the evidence in the comment block there, with the date)')
} else {
  console.log('\nno model failed every attempt')
}
console.log('NOTE: verify anything you keep is worth keeping -- a model can pass N=6 and still be')
console.log('      unreliable. Sample size is small on purpose so a full sweep stays affordable.')