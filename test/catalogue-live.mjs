// The live catalogue, against a local stand-in for the gateway's /v1/models. No network.
//
//   node test/catalogue-live.mjs
import http from 'node:http'

let next = { status: 200, body: '' }
const gateway = http.createServer((req, res) => {
  res.writeHead(next.status, { 'content-type': 'application/json' })
  res.end(next.body)
})
await new Promise((resolve) => gateway.listen(0, '127.0.0.1', resolve))
process.env.CATALOGUE_URL = `http://127.0.0.1:${gateway.address().port}/v1/models`
const cat = await import('../src/catalogue.js')

let failures = 0
const check = (name, ok, detail = '') => {
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' -- ' + detail : ''}`)
  if (!ok) failures++
}
const listing = (ids) => JSON.stringify({ object: 'list', data: ids.map((id) => ({ id, object: 'model' })) })
const ids = (n) => Array.from({ length: n }, (_, i) => `alpha-${String(i + 1).padStart(3, '0')}`)

console.log('1. before the first load, the seed is served')
check('seed is the source', cat.status().source === 'seed')
check('a seed model is known', cat.find('big-pickle') !== null)

console.log('2. the gateway list is passed through whole, with no cap')
next = { status: 200, body: listing(ids(150)) }
let r = await cat.refresh()
check('refresh succeeds', r.ok === true)
check('all 150 are served', cat.count() === 150, `count=${cat.count()}`)
check('the seed is replaced once the gateway has answered', cat.find('big-pickle') === null)
const p = cat.payload('test')
check('every id is github/-prefixed', p.data.length === 150 && p.data.every((m) => m.id.startsWith('github/')))
check('lookup accepts the prefixed and the bare id',
  cat.find('github/alpha-001')?.id === 'alpha-001' && cat.find('alpha-001')?.id === 'alpha-001')

console.log('3. a model the gateway drops is removed, and the change is reported')
next = { status: 200, body: listing(ids(150).filter((id) => id !== 'alpha-007')) }
r = await cat.refresh()
check('alpha-007 is no longer served', cat.find('alpha-007') === null)
check('the removal is reported', r.removed.join() === 'alpha-007' && r.added.length === 0)

console.log('4. a model the gateway adds appears on the next refresh')
next = { status: 200, body: listing([...ids(150).filter((id) => id !== 'alpha-007'), 'gamma-new']) }
r = await cat.refresh()
check('gamma-new is served', cat.find('gamma-new') !== null)
check('the addition is reported', r.added.join() === 'gamma-new' && r.removed.length === 0)

console.log('5. a failed refresh keeps the last good list')
const before = cat.count()
next = { status: 500, body: 'upstream down' }
r = await cat.refresh()
check('the refresh reports failure', r.ok === false)
check('the list is unchanged', cat.count() === before && cat.find('gamma-new') !== null)
check('the error is visible in status', cat.status().lastError === 'HTTP 500', String(cat.status().lastError))

console.log('6. an empty or unusable answer keeps the last good list')
for (const [name, body] of [
  ['empty data array', JSON.stringify({ object: 'list', data: [] })],
  ['not JSON', '<html>blocked</html>'],
  ['no data field', JSON.stringify({ models: [] })],
]) {
  next = { status: 200, body }
  const b = cat.count()
  const rr = await cat.refresh()
  check(`${name}: refused, list kept`, rr.ok === false && cat.count() === b, `count=${cat.count()}`)
}

console.log('7. the catalogue recovers when the gateway does')
next = { status: 200, body: listing(ids(150)) }
r = await cat.refresh()
check('recovers and clears the error', r.ok === true && cat.status().lastError === null)

gateway.close()
console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL CATALOGUE CHECKS PASSED')
process.exit(failures ? 1 : 0)
