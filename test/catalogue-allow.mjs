// CATALOGUE_ALLOW is deployment policy: only the named upstream models are served, even
// when the gateway lists more, and the seed list is filtered the same way. The published
// context/output limits are read from the catalogue the client itself loads, never from a
// table in this repository.
//
//   node test/catalogue-allow.mjs
import http from 'node:http'

let listing = { status: 200, body: '' }
const gateway = http.createServer((req, res) => {
  res.writeHead(listing.status, { 'content-type': 'application/json' })
  res.end(listing.body)
})
await new Promise((resolve) => gateway.listen(0, '127.0.0.1', resolve))

process.env.CATALOGUE_URL = `http://127.0.0.1:${gateway.address().port}/v1/models`
process.env.CATALOGUE_ALLOW = 'deepseek-v4.1-flash-free,alpha-001,alpha-003'
process.env.MODELS_CATALOGUE_URL = 'off'

const cat = await import('../src/catalogue.js')

let failures = 0
const check = (name, ok, detail = '') => {
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' -- ' + detail : ''}`)
  if (!ok) failures++
}

console.log('1. the seed list is filtered before the first refresh')
check('an allowed seed model is served', cat.find('deepseek-v4.1-flash-free') !== null)
check('a seed model outside the allowlist is not', cat.find('big-pickle') === null)
check('the allowlist is reported by status', JSON.stringify(cat.status().allow) === '["deepseek-v4.1-flash-free","alpha-001","alpha-003"]')

console.log('2. a refresh keeps only allowed models')
listing = {
  status: 200,
  body: JSON.stringify({
    object: 'list',
    data: ['alpha-001', 'alpha-002', 'alpha-003', 'alpha-004'].map((id) => ({ id, object: 'model' })),
  }),
}
const ping = async (id) => (id === 'alpha-002' ? 'gone' : 'ok')
const r = await cat.refresh({ ping })
check('refresh succeeds', r.ok === true)
check('only the allowed models are listed', cat.count() === 2 &&
  cat.find('alpha-001') !== null && cat.find('alpha-003') !== null)
check('a model that would have been refused is not probed at all', cat.find('alpha-002') === null && cat.find('alpha-004') === null)
check('the skipped count is visible', cat.status().lastPing?.skipped === 2, JSON.stringify(cat.status().lastPing))
check('the served ids are github/-prefixed', cat.payload('t').data.every((m) => m.id.startsWith('github/')))

console.log('3. an allowlist that matches nothing keeps the previous list')
listing = { status: 200, body: JSON.stringify({ object: 'list', data: [{ id: 'zeta-999', object: 'model' }] }) }
const before = cat.count()
const bad = await cat.refresh({ ping })
check('the refresh is reported as failed', bad.ok === false)
check('the previous list survives', cat.count() === before)
check('and the error names the allowlist', /CATALOGUE_ALLOW/.test(cat.status().lastError || ''), cat.status().lastError)

gateway.close()
console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL ALLOWLIST CHECKS PASSED')
process.exit(failures ? 1 : 0)
