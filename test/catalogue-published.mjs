// The advertised context/output limits come from the catalogue the first-party client
// itself loads (models.opencode.ai), not from a table in this repository; an operator's
// measured override wins; and the upstream's Retry-After is read rather than guessed.
//
//   node test/catalogue-published.mjs
import http from 'node:http'
import { spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

const here = path.dirname(fileURLToPath(import.meta.url))
const published = {
  providers: {
    opencode: {
      models: {
        'alpha-001': { limit: { context: 131072, output: 8192 } },
        'alpha-002': { limit: { context: 1048576, output: 32768 } },
        'no-limit-model': {},
      },
    },
  },
}

const gateway = http.createServer((req, res) => {
  if (req.url === '/catalogue.json') {
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify(published))
    return
  }
  res.writeHead(200, { 'content-type': 'application/json' })
  res.end(JSON.stringify({
    object: 'list',
    data: ['alpha-001', 'alpha-002', 'no-limit-model'].map((id) => ({ id, object: 'model' })),
  }))
})
await new Promise((resolve) => gateway.listen(0, '127.0.0.1', resolve))
const base = `http://127.0.0.1:${gateway.address().port}`

process.env.CATALOGUE_URL = `${base}/v1/models`
process.env.MODELS_CATALOGUE_URL = `${base}/catalogue.json`
process.env.CATALOGUE_ALLOW = '*'
process.env.MODEL_CONTEXT_WINDOWS = ''

const cat = await import('../src/catalogue.js')
const { retryAfterSeconds } = await import('../src/fingerprint.js')

let failures = 0
const check = (name, ok, detail = '') => {
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' -- ' + detail : ''}`)
  if (!ok) failures++
}

console.log('1. published limits are advertised as published')
const ok = async () => 'ok'
const r = await cat.refresh({ ping: ok })
check('refresh succeeds', r.ok === true)
const byId = Object.fromEntries(cat.payload('t').data.map((m) => [m.id, m]))
check('the published context window is used',
  byId['github/alpha-001']?.context_window === 131072, String(byId['github/alpha-001']?.context_window))
check('the published output limit is used',
  byId['github/alpha-001']?.max_output_tokens === 8192, String(byId['github/alpha-001']?.max_output_tokens))
check('a different model gets its own published window',
  byId['github/alpha-002']?.context_window === 1048576)
check('a model with no published limit keeps the declared default and invents no output limit',
  byId['github/no-limit-model']?.context_window === 200000 && !('max_output_tokens' in byId['github/no-limit-model']))
check('the fetch is reported by status', typeof cat.status().publishedAt === 'string')

console.log('2. an operator measurement overrides the published number')
// The child starts its own stand-in so the parent's event loop is never needed while
// spawnSync blocks it.
const script = `
  import http from 'node:http'
  const published = { providers: { opencode: { models: { 'alpha-001': { limit: { context: 131072, output: 8192 } } } } } }
  const srv = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(req.url === '/catalogue.json' ? JSON.stringify(published)
      : JSON.stringify({ object: 'list', data: [{ id: 'alpha-001', object: 'model' }] }))
  })
  await new Promise((r) => srv.listen(0, '127.0.0.1', r))
  const base = 'http://127.0.0.1:' + srv.address().port
  process.env.CATALOGUE_URL = base + '/v1/models'
  process.env.MODELS_CATALOGUE_URL = base + '/catalogue.json'
  process.env.CATALOGUE_ALLOW = '*'
  process.env.MODEL_CONTEXT_WINDOWS = 'alpha-001=262144'
  const cat = await import(${JSON.stringify(path.join(here, '..', 'src', 'catalogue.js'))})
  await cat.refresh({ ping: async () => 'ok' })
  const m = cat.payload('t').data.find((x) => x.id === 'github/alpha-001')
  console.log('WINDOW=' + m.context_window)
  srv.close()
`
const child = spawnSync(process.execPath, ['--input-type=module', '-e', script], { encoding: 'utf8' })
check('the measured window wins over the published one',
  /WINDOW=262144/.test(child.stdout || ''), (child.stdout || '').trim().split('\n').pop() + (child.stderr || '').slice(0, 120))

console.log('3. Retry-After is read from the headers, not guessed')
check('seconds', retryAfterSeconds({ 'retry-after': '37551' }) === 37551)
check('an HTTP date becomes a delta',
  Math.abs((retryAfterSeconds({ 'retry-after': new Date(Date.now() + 120000).toUTCString() }) || 0) - 120) <= 2)
check('x-ratelimit-reset as an epoch',
  Math.abs((retryAfterSeconds({ 'x-ratelimit-reset': String(Math.floor(Date.now() / 1000) + 60) }) || 0) - 60) <= 2)
check('absent means unknown, so the caller keeps its own fallback', retryAfterSeconds({}) === null)

gateway.close()
console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL PUBLISHED-LIMIT CHECKS PASSED')
process.exit(failures ? 1 : 0)
