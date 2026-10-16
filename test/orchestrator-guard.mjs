// The orchestrator dispatches REAL runs to a REAL repository. That costs money and cannot
// be undone, so it must be impossible to trigger by accident.
//
// Twice now it nearly was:
//
//   2026-10-16  a local verification relay (DIRECT_MODE=0, RELAY_TOKEN=t) started the
//               orchestrator and dispatched 14 production runs. All 14 died at registration
//               with {"error":"bad token"}. CI minutes burned, and the run list filled with
//               failures that made a working lane system look broken.
//
//   2026-10-17  LANES_ENABLED was turned back ON (lanes are primary again), which removed
//               the "off by default" protection. So the protection now has to be intrinsic
//               rather than depending on a flag: a relay whose callback URL is loopback
//               cannot possibly be served by a runner, so it must never dispatch.
//
// These checks never touch the network. They assert the gating logic refuses, and that the
// refusal happens BEFORE the dispatch endpoint is reached.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const src = (f) => fs.readFileSync(path.join(here, '..', 'src', f), 'utf8')

let failures = 0
const check = (name, cond, detail = '') => {
  if (cond) console.log(`  PASS  ${name}`)
  else { failures++; console.log(`  FAIL  ${name}${detail ? ' -- ' + detail : ''}`) }
}

const config = src('config.js')
const main = src('main.js')
const orch = src('orchestrator.js')

console.log('orchestrator switch')
check('LANES_ENABLED exists in config', /LANES_ENABLED:\s*str\('LANES_ENABLED',\s*'1'\)/.test(config),
  'lanes are primary again, so it defaults ON')
check('main.js still gates the orchestrator on LANES_ENABLED',
  /LANES_ENABLED\)\s*===\s*'1'[\s\S]{0,200}orchestrator\.js/.test(main))
check('main.js explains the opt-in when it is off', /LANES_ENABLED is not 1/.test(main))

console.log('\nloopback guard -- what makes an always-on orchestrator safe')
check('an unroutable callback URL is detected', /UNROUTABLE/.test(orch) && /isUnroutable/.test(orch))
{
  // Extract the pattern from the source and recompile it, so this test cannot drift from
  // the code and cannot pass by merely finding the right words in the file.
  const startAt = orch.indexOf('const UNROUTABLE')
  const endAt = orch.indexOf('const isUnroutable')
  const block = orch.slice(startAt, endAt > startAt ? endAt : undefined)
  const firstSlash = block.indexOf('/')
  const lastSlash = block.lastIndexOf('/i')
  let re = null
  if (firstSlash !== -1 && lastSlash > firstSlash) {
    try { re = new RegExp(block.slice(firstSlash + 1, lastSlash), 'i') } catch { re = null }
  }
  check('the pattern could be extracted from the source', re !== null, block.slice(0, 80))
  if (re) {
    const loopback = ['http://127.0.0.1:26001', '127.0.0.1:8791', 'localhost:3000',
      'http://localhost:25566', '0.0.0.0:80', 'http://[::1]:25566']
    const missed = loopback.filter((c) => !re.test(c))
    check('every loopback form is caught by the pattern', missed.length === 0, JSON.stringify(missed))
    // An empty/missing URL is handled by `!url` in isUnroutable, not by the pattern, so it
    // is asserted against the function's own logic rather than the regex.
    check('an empty URL is handled by the falsy branch, not the pattern',
      /!url\s*\|\|\s*UNROUTABLE\.test/.test(orch) && !re.test(''))
    check('a routable URL is NOT caught', !re.test('http://160.187.210.9:25566'))
  }
  check('the guard is applied to the relay URL', /isUnroutable\(\s*PUBLIC_URL\s*\)/.test(orch))
  check('and it refuses rather than warning',
    /REFUSING TO DISPATCH[\s\S]{0,600}\breturn\b/.test(orch))
}

console.log('\nthe refusal precedes the dispatch endpoint')
{
  const guardAt = orch.indexOf('REFUSING TO DISPATCH')
  const dispatchAt = orch.indexOf('/dispatches')
  check('the guard runs first', guardAt !== -1 && dispatchAt !== -1 && guardAt < dispatchAt,
    `guard@${guardAt} dispatch@${dispatchAt}`)
}

console.log('\nplausible-token guard')
check('a minimum token length is defined', /TOKEN_PLAUSIBLE[\s\S]{0,140}length >= 16/.test(orch))

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL ORCHESTRATOR GUARD CHECKS PASSED')
process.exit(failures ? 1 : 0)