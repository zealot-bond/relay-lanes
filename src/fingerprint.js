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

/**
 * Resolve ZEN_BASE at CALL time, not import time.
 *
 * The worker's import graph reaches this module before applyConfigToEnv() has
 * run, so a ZEN_BASE that came from start.properties or baked-credentials.json
 * was invisible here and the lane silently used the built-in default. The
 * constant above is kept because it is exported, but every request path uses
 * this function.
 */
export const zenBase = () => process.env.ZEN_BASE || ZEN_BASE

// The gateway rejects a request that does not advertise these four tools, so the
// relay declares them itself and executes them server-side. The calling agent
// therefore never sees a tool it cannot run.
export const FINGERPRINT_TOOLS = ['bash', 'glob', 'grep', 'read']

/**
 * The tools the gateway requires, described as genuinely usable.
 *
 * These four declarations exist because the gateway rejects a request carrying no
 * tool list at all. Forwarding the client's own tool specs alongside them was
 * tried and measured: upstream answered HTTP 400 "Missing required parameter:
 * `tools[4].type`" for every arrangement tried (any shape, any name casing, any
 * count, with or without a `required` array). Only the unmodified set of four is
 * accepted, so the client's specs cannot be passed through and the four below are
 * the whole tool surface.
 *
 * They were previously described as placeholders that "must never be called",
 * which made the model refuse: it answered "I don't have access to a bash tool"
 * instead of calling one. They are now described as ordinary working tools, with
 * the conventional argument shapes, so a call comes back populated. The tool
 * EXECUTION remains the client's job -- the relay only carries the call.
 */
const FINGERPRINT_TOOL_INFO = {
  bash: {
    description: 'Execute a bash command in a persistent shell session and return its output.',
    parameters: {
      type: 'object',
      properties: {
        command: { type: 'string', description: 'The bash command to execute' },
        description: { type: 'string', description: 'Clear, concise description of what this command does' },
      },
      required: ['command', 'description'],
    },
  },
  glob: {
    description: 'Fast file pattern matching tool that works with any codebase size.',
    parameters: {
      type: 'object',
      properties: {
        pattern: { type: 'string', description: 'The glob pattern to match files against, such as **/*.js' },
        path: { type: 'string', description: 'The directory to search in' },
      },
      required: ['pattern'],
    },
  },
  grep: {
    description: 'Search file contents with a regular expression.',
    parameters: {
      type: 'object',
      properties: {
        pattern: { type: 'string', description: 'The regular expression to search for' },
        path: { type: 'string', description: 'File or directory to search' },
        include: { type: 'string', description: 'File extension filter, such as js or py' },
        output_mode: { type: 'string', description: 'One of content, files_with_matches, or count' },
      },
      required: ['pattern'],
    },
  },
  read: {
    description: 'Read the contents of a file from the filesystem.',
    parameters: {
      type: 'object',
      properties: {
        file_path: { type: 'string', description: 'The absolute path to the file to read' },
        offset: { type: 'number', description: 'Line number to start reading from' },
        limit: { type: 'number', description: 'Number of lines to read' },
      },
      required: ['file_path'],
    },
  },
}

export function fingerprintToolSpecs () {
  return FINGERPRINT_TOOLS.map((name) => ({
    type: 'function',
    function: {
      name,
      description: FINGERPRINT_TOOL_INFO[name].description,
      parameters: FINGERPRINT_TOOL_INFO[name].parameters,
    },
  }))
}

export function fingerprintToolSpecsResponses () {
  return FINGERPRINT_TOOLS.map((name) => ({
    type: 'function',
    name,
    description: FINGERPRINT_TOOL_INFO[name].description,
    parameters: FINGERPRINT_TOOL_INFO[name].parameters,
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

  // Only the STATUS or an ERROR ENVELOPE may declare exhaustion. The previous
  // check scanned the whole raw body, which for a 200 is the model's own answer --
  // so asking "how do I raise my rate limit?" produced a correct reply that was
  // classified as bucket exhaustion, the answer was discarded, and the lane was
  // retired and replaced. Twenty such replies destroyed the whole pool.
  //
  // `raw` is consulted only when the response is not a success, so a success body
  // can never be mistaken for an error.
  if (status < 400) return { kind: 'ok' }

  // Exhaustion is declared by the STATUS or a structured marker, never by prose that
  // may be quoting the user. A 4xx body routinely echoes the request ("invalid
  // prompt: how do I raise my rate limit?"), and matching it retired a healthy lane.
  // Prose is trusted only on server-error statuses, where the gateway is the one
  // speaking.
  const structuredLimit = status === 429 || text.includes('FreeUsageLimitError') ||
    text.includes('rate_limit_error')

  // The gateway reports its own access gate two ways: a FreeTierError envelope,
  // and a prose message from the upstream provider. Checked BEFORE prose rate-limit
  // matching: a 403 gate message that merely mentions "rate limits" is still a gate,
  // not an exhausted bucket -- retiring the lane would not change the fingerprint.
  const gate = text.includes('FreeTierError') ||
    /free tier can only be used from within/i.test(text)
  if (gate && !structuredLimit) return { kind: 'gate' }

  const proseLimit = status >= 500 && /rate[_ -]?limit/i.test(text)
  if (structuredLimit || proseLimit) return { kind: 'limited', retryAfter: null }
  if (status >= 500) return { kind: 'transport' }
  return { kind: 'provider_error' }
}