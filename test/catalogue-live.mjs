// The live catalogue, against a local stand-in for the gateway's /v1/models. The ping is
// injected, so no test ever spends quota on the real upstream. No network.
//
//   node test/catalogue-live.mjs
import http from 'node:http'

let listing = { status: 200, body: '' }
const gateway = http.createServer((req, res) => {
  res.writeHead(listing.status, { 'content-type': 'application/json' })
  res.end(listing.body)
})
await new Promise((resolve) => gateway.listen(0, '127.0.0.1', resolve))
process.env.CATALOGUE_URL = `http://127.0.0.1:${gateway.address().port}/v1/models`
// Deployment policy and the published-limit catalogue are both disabled here: this test
// uses synthetic model ids and must not touch the network.
process.env.CATALOGUE_ALLOW = '*'
process.env.MODELS_CATALOGUE_URL = 'off'
const cat = await import('../src/catalogue.js')
const { buildForTest } = await import('../src/lane.js')

let failures = 0
const check = (name, ok, detail = '') => {
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' -- ' + detail : ''}`)
  if (!ok) failures++
}
const list = (ids) => { listing = { status: 200, body: JSON.stringify({ object: 'list', data: ids.map((id) => ({ id, object: 'model' })) }) } }
const ids = (from, to, prefix = 'alpha') => Array.from({ length: to - from + 1 }, (_, i) => `${prefix}-${String(from + i).padStart(3, '0')}`)
const num = (id) => Number(id.slice(id.lastIndexOf('-') + 1))
const ACCEPTED = ['xhigh', 'high', 'medium', 'low']

// alpha-001..100 answer, and accept four efforts; 101..120 are paid (refuse this relay);
// 121..150 are withdrawn. gamma-new answers at low only.
let verdict = (id, effort) => {
  if (id === 'gamma-new') return effort === null || effort === 'low' ? 'ok' : 'provider_error'
  const n = num(id)
  if (effort === null) return n <= 100 ? 'ok' : n <= 120 ? 'provider_error' : 'gone'
  return n <= 100 && ACCEPTED.includes(effort) ? 'ok' : 'provider_error'
}
const ping = async (id, effort) => verdict(id, effort)

console.log('1. before the first load, the seed is served')
check('seed is the source', cat.status().source === 'seed')
check('a seed model is known', cat.find('big-pickle') !== null)

console.log('2. only models that answer a ping are listed, each with its measured efforts')
list(ids(1, 150))
let r = await cat.refresh({ ping })
check('refresh succeeds', r.ok === true)
check('the 100 that answered are served', cat.count() === 100, `count=${cat.count()}`)
check('paid models (refused) are not served', cat.find('alpha-101') === null && cat.find('alpha-120') === null)
check('withdrawn models are not served', cat.find('alpha-121') === null)
check('the seed is replaced once the gateway has answered', cat.find('big-pickle') === null)
check('the efforts each model accepts are its own, highest first',
  JSON.stringify(cat.find('alpha-001').efforts) === JSON.stringify(ACCEPTED))
check('the default effort is the highest it accepts', cat.find('alpha-001').defaultEffort === 'xhigh')
const p = cat.payload('test')
const bases = p.data.filter((m) => !m.id.includes(':'))
const variants = p.data.filter((m) => m.id.includes(':'))
check('every id is github/-prefixed', p.data.every((m) => m.id.startsWith('github/')))
check('each answering model is listed bare, with its default',
  bases.length === 100 && bases.every((m) => m.default_reasoning_effort === 'xhigh'))
check('each accepted effort is listed as a variant',
  variants.length === 400 && variants.some((m) => m.id === 'github/alpha-001:low'))
check('an effort the model refused is not listed', !p.data.some((m) => m.id === 'github/alpha-001:none'))
check('the ping tally is reported',
  cat.status().lastPing?.answering === 100 && cat.status().lastPing?.tested === 150 && cat.status().lastPing?.efforts?.accepted === 400,
  JSON.stringify(cat.status().lastPing?.efforts))

console.log('3. resolving a requested id')
check('a bare id resolves to the default (no effort named)',
  cat.resolve('github/alpha-001')?.effort === null && cat.resolve('github/alpha-001')?.entry.defaultEffort === 'xhigh')
check('a variant id resolves to its effort', cat.resolve('github/alpha-001:high')?.effort === 'high')
check('an effort the model refused does not resolve', cat.resolve('github/alpha-001:none') === null)
check('an unknown model does not resolve, with or without an effort',
  cat.resolve('github/nope') === null && cat.resolve('github/nope:low') === null)
check('the bare lookup accepts the prefixed id', cat.find('github/alpha-001')?.id === 'alpha-001')

console.log('4. a model the gateway drops is removed, and the change is reported')
list(ids(1, 150).filter((id) => id !== 'alpha-050'))
r = await cat.refresh({ ping })
check('alpha-050 is no longer served', cat.find('alpha-050') === null)
check('the removal is reported', r.removed.join() === 'alpha-050' && r.added.length === 0)
check('the count follows', cat.count() === 99)

console.log('5. a model the gateway adds is listed with the efforts it accepts')
list([...ids(1, 150).filter((id) => id !== 'alpha-050'), 'gamma-new'])
r = await cat.refresh({ ping })
check('gamma-new is served', cat.find('gamma-new') !== null)
check('gamma-new is listed with only the one effort it accepts',
  JSON.stringify(cat.find('gamma-new')?.efforts) === '["low"]' && cat.find('gamma-new')?.defaultEffort === 'low')
check('the addition is reported', r.added.join() === 'gamma-new')

console.log('6. a rate-limited ping does not evict a model that was listed')
verdict = (id, effort) => {
  if (id === 'alpha-010' && effort === null) return 'limited'
  if (id === 'delta-new') return 'limited'
  return verdict0(id, effort)
}
const verdict0 = (id, effort) => {
  if (id === 'gamma-new') return effort === null || effort === 'low' ? 'ok' : 'provider_error'
  const n = num(id)
  if (effort === null) return n <= 100 ? 'ok' : n <= 120 ? 'provider_error' : 'gone'
  return n <= 100 && ACCEPTED.includes(effort) ? 'ok' : 'provider_error'
}
list([...ids(1, 100), 'gamma-new', 'delta-new'])
r = await cat.refresh({ ping })
check('the refresh succeeds (2 of 102 limited is not a quota problem)', r.ok === true)
check('alpha-010 stays listed with the efforts it had', cat.find('alpha-010') !== null &&
  JSON.stringify(cat.find('alpha-010').efforts) === JSON.stringify(ACCEPTED))
check('a new model that was only rate-limited is not added', cat.find('delta-new') === null)

console.log('7. a refresh where most pings are rate-limited keeps the last good list')
verdict = () => 'limited'
const before7 = cat.count()
r = await cat.refresh({ ping })
check('the refresh reports failure', r.ok === false)
check('the list is unchanged', cat.count() === before7)
check('the reason is visible', /rate-limited/.test(cat.status().lastError || ''), String(cat.status().lastError))

console.log('8. a refresh where nothing answers keeps the last good list')
verdict = () => 'gone'
r = await cat.refresh({ ping })
check('the refresh reports failure', r.ok === false)
check('the list is unchanged', cat.count() === before7)
check('the reason is visible', /none of .* answered a ping/.test(cat.status().lastError || ''))

console.log('9. a failed gateway fetch keeps the last good list')
verdict = (id, effort) => verdict0(id, effort)
for (const [name, l] of [
  ['HTTP 500', { status: 500, body: 'upstream down' }],
  ['empty data array', { status: 200, body: JSON.stringify({ object: 'list', data: [] }) }],
  ['not JSON', { status: 200, body: '<html>blocked</html>' }],
  ['no data field', { status: 200, body: JSON.stringify({ models: [] }) }],
]) {
  listing = l
  const b = cat.count()
  const rr = await cat.refresh({ ping })
  check(`${name}: refused, list kept`, rr.ok === false && cat.count() === b, `count=${cat.count()}`)
}

console.log('10. the catalogue recovers when everything is healthy again')
list(ids(1, 150))
r = await cat.refresh({ ping })
check('recovers and clears the error', r.ok === true && cat.status().lastError === null)
check('the count is back to the answering set', cat.count() === 100)

console.log('11. a request carries the effort it names, or the model default')
const body = (extra) => buildForTest({ model: 'big-pickle', messages: [{ role: 'user', content: 'x' }], ...extra })
check('an explicit effort is sent upstream', body({ reasoningEffort: 'low' }).reasoning_effort === 'low')
check('no effort is sent when none is set, so the request is unchanged', body({}).reasoning_effort === undefined)

console.log('12. an effort probe that is rate-limited is retried, not read as a refusal')
const attempts = new Map()
const flakyPing = async (id, effort) => {
  if (id !== 'flaky-new') return verdict(id, effort)
  if (effort === null) return 'ok'
  const n = (attempts.get(effort) || 0) + 1
  attempts.set(effort, n)
  if (effort !== 'low') return 'provider_error'
  return n === 1 ? 'limited' : 'ok'
}
list([...ids(1, 100), 'flaky-new'])
r = await cat.refresh({ ping: flakyPing, retryMs: 10 })
check('flaky-new is listed with the effort that succeeded on retry',
  JSON.stringify(cat.find('flaky-new')?.efforts) === '["low"]')
check('the retry is counted', cat.status().lastPing?.efforts?.retried === 1, JSON.stringify(cat.status().lastPing?.efforts))
check('the outcome of every effort probe is reported by kind',
  cat.status().lastPing?.efforts?.byKind?.ok > 0 && cat.status().lastPing?.efforts?.byKind?.limited === undefined)

console.log('13. the advertised window is not a hardcoded per-model claim')
const known = new Set(['big-pickle', 'mimo-v2.6-flash-free', 'deepseek-v4.1-flash-free', 'beam-free', 'mystery-free'])
verdict = (id, effort) => (known.has(id) ? (effort === null ? 'ok' : 'provider_error') : verdict0(id, effort))
list([...known])
r = await cat.refresh({ ping })
const byId = Object.fromEntries(cat.payload('t').data.map((m) => [m.id, m]))
check('with no published limit and no measurement, every model keeps the declared default',
  byId['github/big-pickle']?.context_window === 200000 &&
  byId['github/mimo-v2.6-flash-free']?.context_window === 200000 &&
  byId['github/mystery-free']?.context_window === 200000)
check('no output limit is invented when the catalogue publishes none',
  !('max_output_tokens' in byId['github/big-pickle']) && !('max_output_tokens' in byId['github/beam-free']))
check('every entry carries a name', Object.values(byId).every((m) => typeof m.name === 'string' && m.name.length > 0))

console.log('14. a context refusal lowers what is advertised, for the rest of the generation')
const before = cat.find('beam-free').contextWindow
const lowered = cat.noteContextRefusal('beam-free')
check('the refusal halves the window', lowered === before / 2 && cat.find('beam-free').contextWindow === before / 2,
  `${before} -> ${lowered}`)
check('the lowering is visible to a client listing', cat.payload('t').data.find((m) => m.id === 'github/beam-free')?.context_window === before / 2)
check('the lowering survives a refresh', await (async () => { await cat.refresh({ ping }); return cat.find('beam-free').contextWindow === before / 2 })())
check('an unknown model is not invented by a refusal', cat.noteContextRefusal('not-a-model') === null)
check('the floor is respected', (() => {
  for (let i = 0; i < 40; i++) cat.noteContextRefusal('beam-free')
  return cat.find('beam-free').contextWindow >= 32768
})())

gateway.close()
console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL CATALOGUE CHECKS PASSED')
process.exit(failures ? 1 : 0)
