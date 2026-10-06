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
  PORT: num('PORT', 8791),
  HOST: str('HOST', '0.0.0.0'),
  RELAY_TOKEN: str('RELAY_TOKEN', 'relay'),
  MAX_LANES: num('MAX_LANES', 20),
  MAX_HOLD_MS: num('MAX_HOLD_MS', 180000),
  RETRY_LIMIT: num('RETRY_LIMIT', 3),
  GH_TOKEN: str('GH_TOKEN', ''),
  GH_REPO: str('GH_REPO', ''),
  GH_WORKFLOW: str('GH_WORKFLOW', 'lane.yml'),
  GH_REF: str('GH_REF', 'main'),
  LANES_TARGET: num('LANES_TARGET', 20),
  RELAY_PUBLIC_URL: str('RELAY_PUBLIC_URL', ''),
  ORCH_TICK_MS: num('ORCH_TICK_MS', 20000),
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