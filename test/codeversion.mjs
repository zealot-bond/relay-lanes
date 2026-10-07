// The lane-code fingerprint must change when -- and only when -- lane code changes.
//
// A fingerprint that never changes is worse than none: it would report "all
// current" while the runners executed stale code, which is the exact failure it
// exists to expose. So this checks the negative direction as hard as the positive.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { computeCodeVersion, LANE_FILES, CODE_VERSION } from '../src/codeversion.js'

let failures = 0
const check = (name, cond, detail = '') => {
  if (cond) console.log(`  PASS  ${name}`)
  else { failures++; console.log(`  FAIL  ${name}${detail ? ' -- ' + detail : ''}`) }
}

const src = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../src')
const copyTree = () => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'codever-'))
  for (const f of fs.readdirSync(src)) fs.copyFileSync(path.join(src, f), path.join(d, f))
  return d
}

console.log('determinism')
check('same tree hashes identically', computeCodeVersion(src) === computeCodeVersion(src))
const clone = copyTree()
check('an exact copy of the tree matches the original',
  computeCodeVersion(clone) === CODE_VERSION, `${computeCodeVersion(clone)} vs ${CODE_VERSION}`)
check('hash is 12 hex chars', /^[0-9a-f]{12}$/.test(CODE_VERSION), CODE_VERSION)

console.log('\nsensitivity (every lane-side file must matter)')
for (const f of LANE_FILES) {
  const d = copyTree()
  const p = path.join(d, f)
  fs.writeFileSync(p, fs.readFileSync(p, 'utf8') + '\n// drift\n')
  check(`editing ${f} changes the version`, computeCodeVersion(d) !== CODE_VERSION)
}

console.log('\ndrift detection')
const d2 = copyTree()
fs.writeFileSync(path.join(d2, 'lane.js'), fs.readFileSync(path.join(d2, 'lane.js'), 'utf8').replace('stream: true', 'stream: false'))
check('a one-token change to lane.js is detected', computeCodeVersion(d2) !== CODE_VERSION)

const d3 = copyTree()
fs.unlinkSync(path.join(d3, 'toolmerge.js'))
check('a lane missing toolmerge.js (as the runner repo was) is detected', computeCodeVersion(d3) !== CODE_VERSION)

console.log('\nnon-lane files must NOT affect it')
const d4 = copyTree()
fs.writeFileSync(path.join(d4, 'server.js'), fs.readFileSync(path.join(d4, 'server.js'), 'utf8') + '\n// relay-only change\n')
fs.writeFileSync(path.join(d4, 'orchestrator.js'), fs.readFileSync(path.join(d4, 'orchestrator.js'), 'utf8') + '\n// relay-only change\n')
check('relay-only edits (server.js, orchestrator.js) leave it unchanged',
  computeCodeVersion(d4) === CODE_VERSION)

console.log('\ncoverage')
// Every local module a lane imports must be in LANE_FILES, or its staleness is invisible.
const imports = (file) => [...fs.readFileSync(path.join(src, file), 'utf8').matchAll(/from\s+'\.\/([\w-]+\.js)'/g)].map((m) => m[1])
const seen = new Set()
const walk = (f) => { if (seen.has(f)) return; seen.add(f); for (const i of imports(f)) walk(i) }
walk('worker.js')
const missing = [...seen].filter((f) => !LANE_FILES.includes(f))
check('LANE_FILES covers everything worker.js transitively imports',
  missing.length === 0, `not covered: ${missing.join(', ')}`)
check('worker.js does not pull in relay-only modules',
  ![...seen].some((f) => ['server.js', 'queue.js', 'orchestrator.js', 'main.js'].includes(f)),
  [...seen].join(','))

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL CODE-VERSION CHECKS PASSED')
process.exit(failures ? 1 : 0)
