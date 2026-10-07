// Configuration resolution.
//
// Order, highest first:
//   1. real environment variables (a panel operator can always override)
//   2. start.properties in the working directory (editable after upload)
//   3. baked-credentials.json (baked at build time)
//
// Reading it here rather than in server.js means the values are resolved before
// any module captures them at import time, which is what caused an earlier build
// to ignore a baked PORT.

import fs from 'node:fs'
import path from 'node:path'

const loadProperties = (file) => {
  const out = {}
  let raw = ''
  try { raw = fs.readFileSync(file, 'utf-8') } catch { return out }
  for (const line of raw.split(/\r?\n/)) {
    const t = line.trim()
    if (!t || t.startsWith('#') || t.startsWith('!')) continue
    const eq = t.indexOf('=')
    const co = t.indexOf(':')
    let idx = -1
    if (eq !== -1 && co !== -1) idx = Math.min(eq, co)
    else if (eq !== -1) idx = eq
    else if (co !== -1) idx = co
    if (idx === -1) out[t] = ''
    else out[t.slice(0, idx).trim()] = t.slice(idx + 1).trim()
  }
  return out
}

const loadBaked = () => {
  try {
    return JSON.parse(fs.readFileSync(path.resolve(process.cwd(), 'baked-credentials.json'), 'utf-8')) || {}
  } catch { return {} }
}

const props = loadProperties(path.resolve(process.cwd(), 'start.properties'))
const baked = loadBaked()

/**
 * Build identity, stamped at pack time.
 *
 * Present because "which build is actually running?" was answered wrongly three
 * times: the panel was diagnosed against a stale jar twice, and a symptom was
 * attributed to the wrong fix. The id is in the startup banner, /health and
 * /v1/models, so the running code can always be identified without guessing.
 */
const BUILD = (() => {
  try {
    const raw = fs.readFileSync(path.resolve(process.cwd(), 'build-info.json'), 'utf-8')
    const info = JSON.parse(raw) || {}
    return { id: String(info.id || 'unknown'), built: String(info.built || 'unknown') }
  } catch { return { id: 'dev', built: 'unpacked' } }
})()

/** env wins, then start.properties, then baked. */
const str = (key, fallback = '') => {
  const e = process.env[key]
  if (typeof e === 'string' && e.length > 0) return e
  if (typeof props[key] === 'string' && props[key].length > 0) return props[key]
  const b = baked[key]
  return typeof b === 'string' && b.length > 0 ? b : fallback
}

const num = (key, fallback) => {
  const v = Number.parseInt(str(key, ''), 10)
  return Number.isFinite(v) && v > 0 ? v : fallback
}

export const config = {
  BUILD_ID: BUILD.id,
  BUILD_BUILT: BUILD.built,
  PORT: num('PORT', 8791),
  HOST: str('HOST', '0.0.0.0'),
  RELAY_TOKEN: str('RELAY_TOKEN', 'relay'),
  MAX_LANES: num('MAX_LANES', 20),
  // How long a client may be held before we give up and answer honestly. Long
  // generations legitimately take minutes, so this must exceed the upstream
  // inactivity timeout; otherwise a slow-but-healthy answer is reported as a
  // failure while the model is still writing.
  MAX_HOLD_MS: num('MAX_HOLD_MS', 600000),
  RETRY_LIMIT: num('RETRY_LIMIT', 3),
  GH_TOKEN: str('GH_TOKEN', ''),
  GH_REPO: str('GH_REPO', ''),
  GH_WORKFLOW: str('GH_WORKFLOW', 'lane.yml'),
  GH_REF: str('GH_REF', 'main'),
  LANES_TARGET: num('LANES_TARGET', 20),
  RELAY_PUBLIC_URL: str('RELAY_PUBLIC_URL', ''),
  ORCH_TICK_MS: num('ORCH_TICK_MS', 20000),
  // Everything below is read by a module that used to fall back to its own
  // `process.env.X || default` at import time. Because ES imports are hoisted,
  // those reads happened BEFORE applyConfigToEnv() ran, so any value that lived
  // only in baked-credentials.json was silently ignored -- the same class of bug
  // the comment at the top of this file says was fixed for PORT, still present
  // for every key that was not listed here.
  LANES_STANDBY: num('LANES_STANDBY', 0),          // 0 -> defaults to LANES_TARGET
  DISPATCH_BUDGET: num('DISPATCH_BUDGET', 400),
  DISPATCH_BURST: num('DISPATCH_BURST', 20),
  DISPATCH_SPACING_MS: num('DISPATCH_SPACING_MS', 700),
  DISPATCH_COOLDOWN_MS: num('DISPATCH_COOLDOWN_MS', 30000),
  LANE_FRESH_MS: num('LANE_FRESH_MS', 120000),
  MODEL_HEALTH_THRESHOLD: num('MODEL_HEALTH_THRESHOLD', 3),
  MODEL_QUARANTINE_MS: num('MODEL_QUARANTINE_MS', 120000),
  TOOL_MODE: str('TOOL_MODE', 'passthrough'),
  TOOL_ROUNDS: num('TOOL_ROUNDS', 2),
  FIRST_BYTE_TIMEOUT_MS: num('FIRST_BYTE_TIMEOUT_MS', 600000),
  STREAM_IDLE_TIMEOUT_MS: num('STREAM_IDLE_TIMEOUT_MS', 120000),
  // Comment frame sent to a streaming client while the relay waits upstream, so an
  // intermediary's idle timer does not silently drop a legitimately slow prefill.
  STREAM_KEEPALIVE_MS: num('STREAM_KEEPALIVE_MS', 15000),
  // Ceiling on SSE data held for a client that is not reading. Past this the client
  // is treated as gone. Bounds memory at MAX_BACKLOG_BYTES per in-flight stream.
  MAX_BACKLOG_BYTES: num('MAX_BACKLOG_BYTES', 4 * 1024 * 1024),
  // Reasoning tokens are drawn from the same budget as the answer. Measured on
  // muse-spark-1.3: 800/1200/2000 all produced an EMPTY completion for a ~200-word
  // prompt, while 4000 produced 215 words. A floor below that does not shorten an
  // answer, it deletes it -- which reads as a flaky upstream, not a budget problem.
  MIN_OUTPUT_TOKENS: num('MIN_OUTPUT_TOKENS', 4096),
  DEFAULT_OUTPUT_TOKENS: num('DEFAULT_OUTPUT_TOKENS', 32768),
  ATTEMPT_HARD_MS: num('ATTEMPT_HARD_MS', 300000),
  ATTEMPT_MAX_MS: num('ATTEMPT_MAX_MS', 1800000),
  DELTA_BATCH_MS: num('DELTA_BATCH_MS', 40),
  // Lane liveness, shared by selection, attempt supervision and the claim reaper.
  LANE_STALE_MS: num('LANE_STALE_MS', 45000),
  // How long a claim may sit before its holder is judged on liveness, so a lane
  // that claimed a moment ago is not requeued out from under itself.
  CLAIM_GRACE_MS: num('CLAIM_GRACE_MS', 15000),
}

/**
 * Push resolved values into process.env so any module that still reads
 * process.env directly sees the same answer.
 */
export function applyConfigToEnv () {
  for (const [k, v] of Object.entries(config)) {
    if (v !== '' && !process.env[k]) process.env[k] = String(v)
  }
}