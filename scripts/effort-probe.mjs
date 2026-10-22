// Which reasoning_effort values does the gateway accept, per model, and what does each cost?
//
//   node scripts/effort-probe.mjs <model> [...]
//
// Raw requests with the same fingerprint the lanes use, so run it where the quota is: on a
// runner (see .github/workflows/effort-probe.yml), not on a machine whose free bucket is spent.
// Reports status, reasoning length, answer length and time to the first byte for each effort.
import { headers, zenBase, CHAT_PATH, fingerprintToolSpecs, newSessionId, newRequestId } from '../src/fingerprint.js'

const EFFORTS = ['default', 'none', 'minimal', 'low', 'medium', 'high', 'max', 'xhigh']
const models = process.argv.slice(2)
if (!models.length) { console.error('usage: node scripts/effort-probe.mjs <model> [...]'); process.exit(2) }

async function run (model, effort) {
  const body = {
    model,
    messages: [{ role: 'user', content: 'Reply with the single word: pong' }],
    stream: true,
    tools: fingerprintToolSpecs(),
    max_tokens: 4096,
    ...(effort === 'default' ? {} : { reasoning_effort: effort }),
  }
  const t0 = Date.now()
  const r = await fetch(zenBase() + CHAT_PATH, {
    method: 'POST',
    headers: { ...headers({ sessionId: newSessionId(), requestId: newRequestId(), protocol: 'chat' }), 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) return { status: r.status, err: (await r.text()).replace(/\s+/g, ' ').slice(0, 130) }
  let buf = '', reason = 0, content = 0, first = null
  const dec = new TextDecoder()
  for await (const part of r.body) {
    buf += dec.decode(part, { stream: true })
    let i
    while ((i = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, i).trim()
      buf = buf.slice(i + 1)
      if (!line.startsWith('data: ')) continue
      const d = line.slice(6)
      if (d === '[DONE]') continue
      let j
      try { j = JSON.parse(d) } catch { continue }
      const dl = j.choices?.[0]?.delta || {}
      const rc = (dl.reasoning_content || '') + (typeof dl.reasoning === 'string' ? dl.reasoning : '')
      if (rc) { reason += rc.length; first ??= Date.now() - t0 }
      if (dl.content) { content += dl.content.length; first ??= Date.now() - t0 }
    }
  }
  return { status: 200, reason, content, first, ms: Date.now() - t0 }
}

for (const model of models) {
  console.log(`\n## ${model}`)
  for (const effort of EFFORTS) {
    const x = await run(model, effort)
    const label = effort.padEnd(8)
    if (x.status !== 200) console.log(`  ${label} HTTP ${x.status}  ${x.err}`)
    else console.log(`  ${label} ok  first=${String(x.first).padStart(5)}ms  total=${String(x.ms).padStart(6)}ms  reasoning=${String(x.reason).padStart(5)}  answer=${String(x.content).padStart(4)}`)
  }
}
