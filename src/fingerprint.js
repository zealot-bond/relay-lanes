// Fingerprint the OpenCode Zen free-tier gateway requires, plus the session-id
// format it validates.
//
// Two failure modes must be kept distinct:
//   403 FreeTierError  -> our fingerprint is wrong (client gate). Never a 429.
//   429 FreeUsageLimitError -> genuine quota exhaustion for this egress IP.
//
// Treating the first as the second wastes buckets; treating the second as the
// first hides real exhaustion. So they are separate codes everywhere.

import crypto from 'node:crypto'

const B62 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'

export const LITERAL_KEY = 'public'
export const OPENCODE_UA = 'opencode/1.18.34 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14'
export const OPENCODE_CLIENT = 'cli'
export const OPENCODE_PROJECT = 'global'

export const ZEN_BASE = process.env.ZEN_BASE || 'https://opencode.ai/zen'
export const RESPONSES_PATH = '/v1/responses'
export const CHAT_PATH = '/v1/chat/completions'

// The gateway rejects a request that does not advertise these four tools, so the
// relay declares them itself and executes them server-side. The calling agent
// therefore never sees a tool it cannot run.
export const FINGERPRINT_TOOLS = ['bash', 'glob', 'grep', 'read']

export function fingerprintToolSpecs () {
  return FINGERPRINT_TOOLS.map((name) => ({
    type: 'function',
    function: {
      name,
      description:
        `Placeholder for the "${name}" tool. It is NOT available in this session and must never be called: ` +
        'this declaration only exists to satisfy the upstream API contract. Calling it has no effect and any such call is discarded.',
      parameters: { type: 'object', properties: {} },
    },
  }))
}

export function fingerprintToolSpecsResponses () {
  return FINGERPRINT_TOOLS.map((name) => ({
    type: 'function',
    name,
    description:
      `Placeholder for the "${name}" tool. It is NOT available in this session and must never be called: ` +
      'this declaration only exists to satisfy the upstream API contract. Calling it has no effect and any such call is discarded.',
    parameters: { type: 'object', properties: {} },
  }))
}

// OpenCode encodes ids as a 48-bit timestamp, an inverted flag for "ses", 14
// random base62 chars. A random hex string is rejected by the client gate.
let lastMs = 0
let counter = 0
export function opencodeId (prefix) {
  const now = Date.now()
  if (now !== lastMs) { lastMs = now; counter = 0 }
  counter++
  const r = BigInt(now) * BigInt(0x1000) + BigInt(counter)
  const v = prefix === 'ses' ? ~r : r
  const tb = Buffer.alloc(6)
  for (let i = 0; i < 6; i++) tb[i] = Number((v >> BigInt(40 - 8 * i)) & BigInt(0xff))
  const tail = Array.from(crypto.randomBytes(14), (b) => B62[b % 62]).join('')
  return `${prefix}_${tb.toString('hex')}${tail}`
}

export function newSessionId () { return opencodeId('ses') }
export function newRequestId () { return opencodeId('msg') }

export function headers ({ sessionId, requestId, protocol }) {
  return {
    'Content-Type': 'application/json',
    'User-Agent': OPENCODE_UA,
    Authorization: `Bearer ${LITERAL_KEY}`,
    'x-opencode-client': OPENCODE_CLIENT,
    'x-opencode-project': OPENCODE_PROJECT,
    'x-opencode-session': sessionId,
    'x-opencode-request': requestId,
    // Observed in real client traffic; harmless when absent, so it is sent to
    // stay as close to a genuine client as possible.
    'x-opencode-session-id': sessionId,
    ...(protocol === 'responses' ? { Accept: 'text/event-stream' } : {}),
  }
}

// Classify an upstream reply. This is the single place that decides whether a
// lane is exhausted, our fingerprint is wrong, or the request merely failed.
export function classifyUpstream (status, raw) {
  const text = typeof raw === 'string' ? raw : ''
  const limited = status === 429 || text.includes('FreeUsageLimitError') ||
    text.includes('rate_limit_error') || /rate limit/i.test(text)
  const gate = !limited && text.includes('FreeTierError')
  if (limited) return { kind: 'limited', retryAfter: null }
  if (gate) return { kind: 'gate' }
  if (status >= 500) return { kind: 'transport' }
  if (status >= 400) return { kind: 'provider_error' }
  return { kind: 'ok' }
}