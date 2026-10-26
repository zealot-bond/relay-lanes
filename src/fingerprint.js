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
// The shape the installed v2 client actually sends (captured 2026-10-26, v2.1.2): the
// user-agent names the client and version, and the project id is a 40-hex SHA-1 rather
// than the literal "global" the older client used.
export const OPENCODE_UA = process.env.OPENCODE_UA || 'opencode/latest/2.1.2/cli'
export const OPENCODE_CLIENT = 'cli'
export const OPENCODE_PROJECT = crypto.createHash('sha1').update('relay-lanes').digest('hex')

// The upstream moved. models.dev -- the catalogue OpenCode v2 itself loads -- declares
// the `opencode` provider as `@ai-sdk/openai-compatible` at
//   https://opencode.ai/inference/v1
// and the installed client (v2.0.22) calls exactly that. This relay was still calling the
// older `https://opencode.ai/zen`, which is the path the reference proxy
// (Steve3184/OpenCodeProxyHub) also hardcodes and which now answers
//   HTTP 403 {"error":{"type":"FreeTierError",
//     "message":"OpenCode's free tier can only be used from within OpenCode."}}
// for every request. Both paths return that here, so switching is not by itself proven to
// fix it -- but calling a path the current client does not use is wrong regardless, and
// /zen/v1/models (the only list reachable without that 403) has shrunk to 86 entries and
// omits models the real catalogue still carries, so it cannot be trusted either.
export const ZEN_BASE = process.env.ZEN_BASE || 'https://opencode.ai/inference'
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
 * tool list at all. Client tool specs are normalized and unioned with these four
 * (keeping bash, glob, grep, read present so the gateway access gate is satisfied).
 *
 * They are described as ordinary working tools with conventional argument shapes,
 * so calls come back populated when tools are used. The tool EXECUTION remains
 * the client's job -- the relay only carries the call.
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
  // The client sends b3/traceparent on every call; they are correlation ids, not auth, but
  // a request without them is one more way to look unlike the client.
  const trace = crypto.randomBytes(16).toString('hex')
  const span = crypto.randomBytes(8).toString('hex')
  return {
    'Content-Type': 'application/json',
    'User-Agent': OPENCODE_UA,
    Authorization: `Bearer ${LITERAL_KEY}`,
    b3: `${trace}-${span}-1-${crypto.randomBytes(8).toString('hex')}`,
    traceparent: `00-${trace}-${span}-01`,
    'x-opencode-client': OPENCODE_CLIENT,
    'x-opencode-project': OPENCODE_PROJECT,
    'x-opencode-session': sessionId,
    // The session id is sent under four names by the current client.
    'x-opencode-session-id': sessionId,
    'x-session-affinity': sessionId,
    'x-session-id': sessionId,
    ...(protocol === 'responses' ? { Accept: 'text/event-stream' } : {}),
  }
}

/**
 * Seconds the upstream asked us to wait, from the response headers.
 *
 * The gateway answers exhaustion with `Retry-After` (measured: 37551 at 13:33 UTC, i.e. the
 * daily reset at midnight UTC) but the relay only ever looked at the body, so it reported a
 * guessed 900s and clients retried long before the window reopened.
 */
export function retryAfterSeconds (headers) {
  if (!headers || typeof headers !== 'object') return null
  const raw = headers['retry-after']
  if (raw !== undefined) {
    const n = Number(raw)
    if (Number.isFinite(n) && n > 0) return Math.ceil(n)
    const t = Date.parse(String(raw))
    if (Number.isFinite(t)) return Math.max(1, Math.ceil((t - Date.now()) / 1000))
  }
  const reset = Number(headers['x-ratelimit-reset'])
  if (Number.isFinite(reset) && reset > 0) {
    // Some providers send an epoch, some a delta. Anything past 1e9 is an epoch.
    return reset > 1e9 ? Math.max(1, Math.ceil(reset - Date.now() / 1000)) : Math.ceil(reset)
  }
  return null
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

  // The model was withdrawn by the provider. This is PERMANENT, and it is checked
  // before every status-driven verdict, because a body saying the model no longer
  // exists is more specific than the status code carrying it. A 429 that arrives with
  // a deprecation notice was previously read as "bucket exhausted" -- retiring a lane
  // and churning the runner pool for a model that will never answer from any IP.
  //
  // Without its own kind this fell through to provider_error, which reads as
  // "retryable in a moment": the client sat through six upstream calls across three
  // lanes, and only then hit the generic quarantine. exo-free and fledge-alpha-free
  // both answered 410 {"type":"ModelDeprecated"} for eight days before being removed
  // from the catalogue (2026-10-15).
  if (status === 404 || status === 410 ||
      /ModelDeprecated|model (?:is )?(?:not found|no longer)|has been deprecated|model_not_found/i.test(text)) {
    return { kind: 'gone', permanent: true }
  }

  // Exhaustion is declared by the STATUS or a structured marker, never by prose that
  // may be quoting the user. A 4xx body routinely echoes the request ("invalid
  // prompt: how do I raise my rate limit?"), and matching it retired a healthy lane.
  // Prose is trusted only on server-error statuses, where the gateway is the one
  // speaking.
  // A 429 is not always a rate limit. ling-3.1-flash-free answered
  //   HTTP 429 {"error":{"type":"server_error","message":"Upstream request failed:
  //             Endpoint is unavailable."}}
  // -- the model's endpoint was down, and the status code was simply wrong. Trusting
  // it retired a lane per request (20 retirements from 10 requests) and made every
  // runner exit, churning the pool for a fault no egress IP can fix. An outage
  // message is a model fault, so it is reported as provider_error, which is retried
  // once and counted toward quarantining the MODEL rather than burning lanes.
  // The prompt is longer than the model's context window. Every lane refuses the same request, so
  // it is not retried, and it says nothing about the model. It gets its own kind so the client is
  // told to compact the conversation, which is what the harness does on this wording.
  if (status >= 400 && status < 500 && /maximum context length|context[ _-]?length[ _-]?exceeded|more tokens than this model|context window (?:exceeded|is too)/i.test(text)) {
    return { kind: 'context' }
  }

  const modelOutage = /endpoint is unavailable|upstream request failed|model is unavailable|temporarily unavailable/i.test(text) &&
    !text.includes('FreeUsageLimitError') && !text.includes('rate_limit_error')
  if (modelOutage) return { kind: 'provider_error', outage: true }

  const structuredLimit = status === 429 || text.includes('FreeUsageLimitError') ||
    text.includes('rate_limit_error')

  // The gateway reports its own access gate two ways: a FreeTierError envelope,
  // and a prose message from the upstream provider. Checked BEFORE prose rate-limit
  // matching: a 403 gate message that merely mentions "rate limits" is still a gate,
  // not an exhausted bucket -- retiring the lane would not change the fingerprint.
  // A blacklisted EGRESS IP is not the same as the fingerprint gate.
  //
  //   "OpenCode's free models aren't available from your current network because we've
  //    detected abusive traffic from this IP address."
  //
  // The gate ("can only be used from within OpenCode") is about WHO is asking, and no
  // other IP helps, so it must not be retried. THIS one is about the IP's REPUTATION,
  // and a different IP answers normally.
  //
  // Reported as `limited` deliberately, because that is the kind the relay already
  // handles correctly: it retires that lane, requeues the work and retries it on
  // another egress, and it does NOT count the failure against the MODEL's health.
  // Classified as `gate` -- which is what every 403 carrying FreeTierError became --
  // the blacklisted lane stayed in the pool failing every request, and the model was
  // quarantined for everyone over an IP-level fault. Measured 2026-10-20: one lane
  // served 174 requests and was then blacklisted mid-run, and the pool never healed.
  const ipBlocked = /aren't available from your current network|detected abusive traffic|abusive traffic from this IP/i.test(text)
  if (ipBlocked) return { kind: 'limited', retryAfter: null, ipBlocked: true }

  const gate = text.includes('FreeTierError') ||
    /free tier can only be used from within/i.test(text)
  if (gate && !structuredLimit) return { kind: 'gate' }

  const proseLimit = status >= 500 && /rate[_ -]?limit/i.test(text)
  if (structuredLimit || proseLimit) return { kind: 'limited', retryAfter: null }
  if (status >= 500) return { kind: 'transport' }

  return { kind: 'provider_error', outage: false }
}