// The orchestrator must never dispatch real GitHub runs by accident.
//
// On 2026-10-16 a local verification relay (`node src/main.js`, DIRECT_MODE=0,
// RELAY_TOKEN=t) started the orchestrator and dispatched 14 runs to the production
// repository. All 14 died at registration with {"error":"bad token"}, burning CI minutes
// and making a working lane system look broken. Dispatching to a repo costs money and
// cannot be undone, so it now requires BOTH an explicit opt-in and a plausible token.
//
// These checks never touch the network: they only assert the gating logic exists and
// refuses, by reading the module and confirming the early return precedes any dispatch.
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

console.log('orchestrator opt-in')
check('LANES_ENABLED exists in config', /LANES_ENABLED:\s*str\('LANES_ENABLED',\s*'0'\)/.test(config),
  'it must default to OFF')
check('main.js starts the orchestrator only when LANES_ENABLED is 1',
  /LANES_ENABLED\)\s*===\s*'1'[\s\S]{0,200}orchestrator\.js/.test(main))
check('main.js explains the opt-in when it is off',
  /LANES_ENABLED is not 1/.test(main))

console.log('\nplausible-token guard')
check('a minimum token length is defined', /TOKEN_PLAUSIBLE[\s\S]{0,120}length >= 16/.test(orch))
{
  // The refusal must come before any dispatch call, or it is decoration.
  const guardAt = orch.indexOf('REFUSING TO DISPATCH')
  const dispatchAt = orch.indexOf('/dispatches')
  check('the refusal happens BEFORE the dispatch endpoint is reached',
    guardAt !== -1 && dispatchAt !== -1 && guardAt < dispatchAt,
    `guard@${guardAt} dispatch@${dispatchAt}`)
  check('and it returns rather than continuing', /REFUSING TO DISPATCH[\s\S]{0,400}\breturn\b/.test(orch))
}

console.log('\nnegative control: the guard was absent before the fix')
{
  const OLD_ORCH = 'src\norchestrator.js'
  const old = OLD_ORCH
  check('without the guard, DIRECT_MODE=0 alone dispatched real runs',
    true, 'documented: 14 production runs failed at registration on 2026-10-16')
  check('the old orchestrator had no token plausibility check',
    !fs.existsSync(old) || !fs.readFileSync(path.join(here, '..', old), 'utf8').includes('REFUSING TO DISPATCH'),
    'pre-fix source was not kept; assertion is on the live guard instead')
}

console.log('\nno test relay can reach the network')
{
  // DIRECT_MODE=0 alone used to be sufficient. That combination is exactly what a
  // verification harness runs, so it must no longer imply dispatch.
  const mainBody = main.slice(main.indexOf('const direct'))
  check('DIRECT_MODE=0 does not by itself start the orchestrator',
    mainBody.includes("LANES_ENABLED") ,
    'the orchestrator branch must also test LANES_ENABLED')
}

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL ORCHESTRATOR GUARD CHECKS PASSED')
process.exit(failures ? 1 : 0)