// Tool specs must reach upstream in the shape the TARGET dialect requires, whatever
// shape the client sent, alongside the gateway's own tool names.
//
// History, because both halves were gotten wrong once and the tests must not
// re-encode a wrong belief:
//
//  * Anything lacking a `function` key was once forwarded untouched, so an
//    Anthropic-style spec ({name, input_schema}) reached upstream with no `type`
//    and no `parameters`. Fixed by normalising every shape.
//
//  * A later round concluded "the gateway rejects every client tool and only
//    accepts its own four". That was drawn from a panel whose runners were still
//    executing the pre-fix converter, and was disproved by calling this code
//    directly: OpenAI- and Anthropic-style client tools are accepted.
//
//  * The gateway's access check needs its four names present EXACTLY as spelled.
//    A case-insensitive de-duplication let a client's `Bash` remove the gateway's
//    `bash`, which answered 403 FreeTierError. Suppression is therefore exact-match.
import { buildForTest } from '../src/lane.js'

let failures = 0
const check = (name, cond, detail = '') => {
  if (cond) console.log(`  PASS  ${name}`)
  else { failures++; console.log(`  FAIL  ${name}${detail ? ' -- ' + detail : ''}`) }
}

const S = (p) => ({ type: 'object', properties: p, required: Object.keys(p) })
const EDIT = S({ file_path: { type: 'string' }, old_string: { type: 'string' }, new_string: { type: 'string' } })
const BASH = S({ command: { type: 'string' }, description: { type: 'string' } })
const GATEWAY = ['bash', 'glob', 'grep', 'read']

const SHAPES = {
  'OpenAI chat-style': [
    { type: 'function', function: { name: 'bash', description: 'Run a command', parameters: BASH } },
    { type: 'function', function: { name: 'edit', description: 'Edit a file', parameters: EDIT } },
  ],
  'Anthropic-style': [
    { name: 'bash', description: 'Run a command', input_schema: BASH },
    { name: 'edit', description: 'Edit a file', input_schema: EDIT },
  ],
  'flat responses-style': [
    { type: 'function', name: 'bash', description: 'Run a command', parameters: BASH },
    { type: 'function', name: 'edit', description: 'Edit a file', parameters: EDIT },
  ],
}

const nameOf = (t) => t.name || t.function?.name
const paramsOf = (t) => t.parameters ?? t.function?.parameters

for (const model of ['big-pickle', 'muse-spark-1.3-contributor-free']) {
  const dialect = model.includes('muse') ? 'responses' : 'chat'
  console.log(`${dialect} dialect (via ${model})`)

  for (const [label, tools] of Object.entries(SHAPES)) {
    const body = buildForTest({ model, messages: [{ role: 'user', content: 'x' }], tools })
    const sent = body.tools || []
    const names = sent.map(nameOf)

    const edit = sent.find((t) => nameOf(t) === 'edit')
    check(`${label}: client tool 'edit' is forwarded`, Boolean(edit), names.join(','))
    check(`${label}: its schema survives with all three required properties`,
      ['file_path', 'old_string', 'new_string'].every((k) => paramsOf(edit)?.required?.includes(k)),
      JSON.stringify(paramsOf(edit)?.required))

    const wellFormed = dialect === 'chat'
      ? (t) => t.type === 'function' && t.function?.name && t.function?.parameters
      : (t) => t.type === 'function' && t.name && t.parameters
    check(`${label}: every spec is valid ${dialect} shape`, sent.every(wellFormed),
      JSON.stringify(sent.filter((t) => !wellFormed(t)).map(nameOf)))

    check(`${label}: gateway tool names all present`, GATEWAY.every((g) => names.includes(g)), names.join(','))
    check(`${label}: no tool name appears twice`, new Set(names).size === names.length, names.join(','))
    check(`${label}: no tool advertised with an empty schema`,
      sent.every((t) => Object.keys(paramsOf(t)?.properties || {}).length > 0),
      sent.filter((t) => !Object.keys(paramsOf(t)?.properties || {}).length).map(nameOf).join(','))

    const bash = sent.find((t) => nameOf(t) === 'bash')
    check(`${label}: bash requires command AND description`,
      paramsOf(bash)?.required?.includes('command') && paramsOf(bash)?.required?.includes('description'),
      JSON.stringify(paramsOf(bash)?.required))
  }

  // The regression: a capitalised client name must not remove the gateway's tool.
  const caps = buildForTest({
    model, messages: [{ role: 'user', content: 'x' }],
    tools: [{ name: 'Bash', description: 'Run', input_schema: BASH }, { name: 'Edit', description: 'Edit', input_schema: EDIT }],
  })
  const capNames = (caps.tools || []).map(nameOf)
  check('capitalised client Bash/Edit: gateway tools stay present (case-exact suppression)',
    GATEWAY.every((g) => capNames.includes(g)) && capNames.includes('Bash') && capNames.includes('Edit'),
    capNames.join(','))

  // Exact-name match is the only thing that suppresses a gateway tool.
  const dup = buildForTest({
    model, messages: [{ role: 'user', content: 'x' }],
    tools: [{ type: 'function', function: { name: 'read', description: 'Read a file', parameters: S({ file_path: { type: 'string' } }) } }],
  })
  const dupNames = (dup.tools || []).map(nameOf)
  check('client tool with the identical name replaces the gateway one, not duplicates it',
    dupNames.filter((n) => n === 'read').length === 1, dupNames.join(','))

  // No client tools at all still yields a usable, gate-satisfying set.
  const none = buildForTest({ model, messages: [{ role: 'user', content: 'x' }] })
  check('no client tools: exactly the four gateway tools',
    (none.tools || []).length === 4 && GATEWAY.every((g) => (none.tools || []).map(nameOf).includes(g)))

  // A model told a tool "must never be called" refuses to call it.
  const descs = (none.tools || []).map((t) => t.description || t.function?.description || '')
  check('gateway tool descriptions are usable (no "never be called" wording)',
    !descs.some((d) => /never be called|NOT available|discarded/i.test(d)) && descs.every((d) => d.trim().length > 10))
  console.log('')
}

console.log(failures ? `${failures} FAILURE(S)` : 'ALL TOOL-SHAPE CHECKS PASSED')
process.exit(failures ? 1 : 0)
