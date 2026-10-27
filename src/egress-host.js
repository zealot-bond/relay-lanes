// Starts and supervises the Bun egress proxy (see src/egress.js for why).
//
// Called once per process: the worker before it registers, and the relay in
// main.js so DIRECT_MODE and DIRECT_FALLBACK also leave through Bun's TLS.
// `zenBase()` resolves at call time, so rewriting process.env.ZEN_BASE here is
// enough -- no module needs to know the proxy exists.

import { spawn } from 'node:child_process'
import net from 'node:net'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { canonicalBase, CANONICAL_PREFIX } from './fingerprint.js'

const here = path.dirname(fileURLToPath(import.meta.url))

function freePort () {
  return new Promise((resolve, reject) => {
    const s = net.createServer()
    s.once('error', reject)
    s.listen(0, '127.0.0.1', () => {
      const { port } = s.address()
      s.close(() => resolve(port))
    })
  })
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

async function healthy (port, tries = 100) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}/__egress_health`, { signal: AbortSignal.timeout(1000) })
      if (r.ok) return true
    } catch {}
    await sleep(50)
  }
  return false
}

/**
 * Probe the Bun binary once so a missing runtime is one clear log line rather
 * than a spawn error per request.
 */
function bunAvailable (bin) {
  return new Promise((resolve) => {
    let done = false
    const fin = (v) => { if (!done) { done = true; resolve(v) } }
    const p = spawn(bin, ['--version'], { stdio: ['ignore', 'pipe', 'ignore'] })
    let out = ''
    p.stdout.on('data', (c) => { out += c })
    p.on('error', () => fin(null))
    p.on('exit', (code) => fin(code === 0 ? out.trim() : null))
    setTimeout(() => { try { p.kill() } catch {} ; fin(null) }, 5000)
  })
}

/**
 * Point ZEN_BASE at a loopback proxy that speaks to opencode.ai through Bun.
 *
 * Returns null (and leaves the config untouched) when:
 *   - EGRESS_ENABLED=0,
 *   - the upstream is not opencode.ai (tests and mocks keep their own base),
 *   - Bun is not installed.
 */
export async function startEgress (opts = {}) {
  if (String(process.env.EGRESS_ENABLED ?? '1') === '0') return null
  const raw = canonicalBase(process.env.ZEN_BASE || 'https://opencode.ai' + CANONICAL_PREFIX)
  let url
  try { url = new URL(raw) } catch { return null }
  if (url.hostname !== 'opencode.ai') return null

  const bin = opts.bin || process.env.BUN_BIN || 'bun'
  const version = await bunAvailable(bin)
  if (!version) {
    console.error(`[egress] ${bin} not found: upstream calls will use Node's TLS and the ` +
      'free tier will answer 429 FreeUsageLimitError for every request, on every IP. ' +
      'Install Bun (https://bun.sh) or set BUN_BIN. See AGENTS.md.')
    return null
  }

  const origin = url.origin
  const prefix = url.pathname.replace(/\/$/, '')
  let port = opts.port || await freePort()
  let child = null
  let stopped = false
  let restarts = 0

  const launch = async () => {
    child = spawn(bin, [path.join(here, 'egress.js')], {
      env: { ...process.env, EGRESS_PORT: String(port), EGRESS_ORIGIN: origin },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    child.stdout.on('data', (c) => {
      const s = String(c).trim()
      if (s) console.log(`[egress] ${s}`)
    })
    child.stderr.on('data', (c) => {
      const s = String(c).trim()
      if (s) console.error(`[egress] ${s}`)
    })
    child.on('exit', (code, sig) => {
      if (stopped) return
      if (restarts >= 5) {
        console.error(`[egress] proxy exited (${code || sig}) and will not be restarted; ` +
          'upstream calls fall back to Node TLS and will be rate-limited')
        return
      }
      restarts++
      const samePort = restarts <= 3
      const next = samePort ? port : null
      setTimeout(async () => {
        if (stopped) return
        if (next === null) port = await freePort()
        console.error(`[egress] proxy exited (${code || sig}); restarting on ${port}`)
        await launch()
        if (!(await healthy(port))) console.error('[egress] restarted proxy did not answer health')
        process.env.ZEN_BASE = `http://127.0.0.1:${port}${prefix}`
        process.env.ZEN_UPSTREAM_BASE = origin + prefix
      }, 250)
    })
    return child
  }

  await launch()
  if (!(await healthy(port))) {
    console.error('[egress] proxy did not become healthy; falling back to Node TLS')
    stopped = true
    try { child.kill() } catch {}
    return null
  }

  process.env.ZEN_BASE = `http://127.0.0.1:${port}${prefix}`
  // Keep the real base reachable: modelsUrl() must derive the listing from opencode.ai,
  // not from the loopback origin, or the catalogue refresh asks the proxy for
  // /inference/openai/v1/models and gets a 404.
  process.env.ZEN_UPSTREAM_BASE = origin + prefix
  const stop = () => { stopped = true; try { child.kill() } catch {} }
  process.once('exit', stop)
  console.log(`[egress] bun ${version}: upstream ${origin}${prefix} -> 127.0.0.1:${port}`)
  return { port, origin, stop }
}
