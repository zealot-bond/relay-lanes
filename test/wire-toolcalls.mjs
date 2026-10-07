// Wire-level regression test for the Edit bug.
//
// The defect was an aliasing bug, not a merge-logic bug, so it was invisible to
// assertions on the merger's return value. This asserts the exact bytes a client
// would reassemble, which is the only place it can be observed.
import { mergeToolDeltas } from '../src/toolmerge.js'

let failures = 0
const check = (name, cond, detail = '') => {
  if (cond) console.log(`  PASS  ${name}`)
  else { failures++; console.log(`  FAIL  ${name}${detail ? ' -- ' + detail : ''}`) }
}

// Replay what the worker now does: lane emits an identity fragment per call, then
// argument fragments across batches; each batch serialised independently.
class Wire {
  constructor () { this.acc = new Map(); this.wire = [] }
  flush (frags) {
    const merged = mergeToolDeltas(frags, this.acc)
    this.wire.push(...merged)
  }
}

// Exactly the sequence the responses dialect produces for ONE edit call.
const w1 = new Wire()
w1.flush([{ index: 0, id: 'fc_1', type: 'function', function: { name: 'edit', arguments: '' } }])
w1.flush([{ index: 0, function: { arguments: '{"file_path":"a' } }])
w1.flush([{ index: 0, function: { arguments: '.txt","old_str' } }])
w1.flush([{ index: 0, function: { arguments: 'ing":"x","new_string":"y"}' } }])

const args = w1.wire.map((f) => f.function.arguments || '').join('')
const id = w1.wire.find((f) => f.id)?.id
const name = w1.wire.find((f) => f.function?.name)?.function.name

console.log('edit call, four batches:')
console.log('  fragments:', JSON.stringify(w1.wire))
check('id preserved once', id === 'fc_1')
check('name preserved once', name === 'edit')
let parsed = null
try { parsed = JSON.parse(args) } catch { /* handled below */ }
check('arguments parse as JSON', parsed !== null, args)
check('no duplicated arguments', args === '{"file_path":"a.txt","old_string":"x","new_string":"y"}', args)
check('file_path present', parsed?.file_path === 'a.txt')
check('old_string present', parsed?.old_string === 'x')
check('new_string present', parsed?.new_string === 'y')
check('one entry per batch per index', w1.wire.every((f) => f.index === 0))

// Two parallel calls must not merge.
const w2 = new Wire()
w2.flush([
  { index: 0, id: 'fc_1', type: 'function', function: { name: 'edit', arguments: '' } },
  { index: 1, id: 'fc_2', type: 'function', function: { name: 'read', arguments: '' } },
])
w2.flush([
  { index: 0, function: { arguments: '{"file_path":"a"}' } },
  { index: 1, function: { arguments: '{"file_path":"b"}' } },
])
const byIndex = new Map()
for (const f of w2.wire) {
  const cur = byIndex.get(f.index) || { name: '', args: '' }
  if (f.function?.name) cur.name += f.function.name
  if (f.function?.arguments) cur.args += f.function.arguments
  byIndex.set(f.index, cur)
}
console.log('\ntwo parallel calls:')
console.log('  reassembled:', JSON.stringify([...byIndex]))
check('two distinct indices', byIndex.size === 2)
let a0 = null, a1 = null
try { a0 = JSON.parse(byIndex.get(0).args); a1 = JSON.parse(byIndex.get(1).args) } catch { /* */ }
check('call 0 args valid', a0?.file_path === 'a', byIndex.get(0)?.args)
check('call 1 args valid', a1?.file_path === 'b', byIndex.get(1)?.args)
check('call 0 named edit', byIndex.get(0)?.name === 'edit')
check('call 1 named read', byIndex.get(1)?.name === 'read')

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL WIRE CHECKS PASSED')
process.exit(failures ? 1 : 0)