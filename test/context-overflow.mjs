// A prompt longer than the model's context window is the client's to fix, not the model's to
// fail on: it is classified as its own kind, reported with the wording the harness recognises,
// and never counted against the model's health.
//
//   node test/context-overflow.mjs
import { classifyUpstream } from '../src/fingerprint.js'
import { noteModelResult, modelHealthOf, contextMessage } from '../src/server.js'

let failures = 0
const check = (name, ok, detail = '') => {
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' -- ' + detail : ''}`)
  if (!ok) failures++
}

const OVERFLOW = JSON.stringify({
  error: {
    message: "Your request resulted in more tokens than this model's maximum context length allows. Please reduce the length of the messages or run compaction.",
    type: 'invalid_request_error',
  },
})

console.log('1. classification')
check('a 400 with the overflow wording is its own kind', classifyUpstream(400, OVERFLOW).kind === 'context',
  JSON.stringify(classifyUpstream(400, OVERFLOW)))
check('an ordinary 400 is still a provider error',
  classifyUpstream(400, '{"error":{"message":"Missing required parameter: tools[4].type"}}').kind === 'provider_error')
check('a rate limit is still a rate limit', classifyUpstream(429, '{"error":{"type":"FreeUsageLimitError"}}').kind === 'limited')
check('a successful body that mentions the wording is never an error', classifyUpstream(200, OVERFLOW).kind === 'ok')

console.log('2. the model is not penalised for an oversized prompt')
const model = 'context-test-model'
const result = { kind: 'context', status: 400, raw: OVERFLOW }
for (let i = 0; i < 5; i++) noteModelResult(model, result, { log: false })
const health = modelHealthOf(model)
check('five overflows do not quarantine the model', !health || health.quarantined !== true, JSON.stringify(health))
check('and do not count as failures', !health || (health.fails ?? 0) === 0, JSON.stringify(health))

console.log('3. the client is told to compact, in the wording the harness recognises')
const message = contextMessage(result, 'glm-5.3-flash-free')
check('names the model', message.startsWith('github/glm-5.3-flash-free:'))
check('keeps the overflow wording', /maximum context length/i.test(message), message.slice(0, 120))
check('tells the client to compact', /compact/i.test(message))

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL CONTEXT-OVERFLOW CHECKS PASSED')
process.exit(failures ? 1 : 0)
