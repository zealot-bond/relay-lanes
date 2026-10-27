// A spent free-tier window that reopens hours later is not a lane fault.
//
// The relay retires a lane on `limited` because the next runner has a different egress
// IP and therefore a different bucket. That is right for a short window.
//
// This breaker existed because on 2026-10-26 the upstream answered every request from
// this host and from every runner at once with
//
//   429 FreeUsageLimitError, Retry-After: 37551   (the reset at 00:00 UTC)
//
// which looked like one window shared by all IPs. It was not: measured the same day by
// bisection, that 429 is the free-tier GATE's fake quota, returned to any request that
// is not shaped like the first-party client (Bun's TLS fingerprint, an `<env>` block with
// a working directory, the four gateway tool names). A real client on the same IP answered
// 200 while every lane was "exhausted". So the premise is invalid and the breaker defaults
// OFF (EXHAUSTION_GLOBAL_S=0); the gate is prevented, not classified -- see
// src/egress.js and ensureEnvBlock() in lane.js.
//
// The mechanism is kept for a deployment that has measured a genuinely global window: a
// `limited` result whose Retry-After is at least EXHAUSTION_GLOBAL_S keeps the lane and
// stops upstream calls until it passes. A short Retry-After always keeps the per-IP
// behaviour, because another IP genuinely can answer there.
import { config } from './config.js'

const state = {
  until: 0,
  retryAfterS: 0,
  model: null,
  at: null,
  hits: 0,
}

/**
 * Is this outcome evidence that every egress IP sees the same closed window?
 *
 * Only `limited` qualifies: `ip_blocked` is per-IP by definition, and a provider error
 * carrying a Retry-After is still about the request, not the bucket.
 */
export function isGlobal (result, thresholdS = config.EXHAUSTION_GLOBAL_S) {
  const threshold = Number(thresholdS) || 0
  // 0 (or a non-positive value) means the concept is disabled: no Retry-After is long
  // enough to close the relay. Without this guard every `limited` at threshold 0 would
  // be "global", including the ordinary per-IP ones the lane retry exists for.
  if (threshold <= 0) return false
  if (!result || result.kind !== 'limited') return false
  const s = Number(result.retryAfter)
  return Number.isFinite(s) && s >= threshold
}

/** Arm the breaker from a limited result's own Retry-After. */
export function note (retryAfterS, model) {
  const s = Math.max(1, Math.ceil(Number(retryAfterS)))
  const wasActive = active()
  state.until = Date.now() + s * 1000
  state.retryAfterS = s
  state.model = model
  state.at = new Date().toISOString()
  state.hits++
  if (!wasActive) {
    console.log(`[relay] upstream window exhausted for ${model}: Retry-After ${s}s ` +
      `(until ${new Date(state.until).toISOString()}). Lanes stay up; no upstream call ` +
      'is made until then, because every egress IP sees the same window.')
  }
  return state.retryAfterS
}

export function active () {
  return state.until > Date.now()
}

/** The answer to give a client without touching a lane or the upstream. */
export function result (model) {
  return {
    kind: 'limited',
    status: 429,
    ms: 0,
    data: null,
    laneId: null,
    retryAfter: state.retryAfterS,
    raw: `Free usage limit reached; the free-tier window reopens at ` +
      `${new Date(state.until).toISOString()} (${state.retryAfterS}s).`,
  }
}

export function status () {
  return {
    active: active(),
    until: state.until ? new Date(state.until).toISOString() : null,
    retryAfterS: state.retryAfterS,
    model: state.model,
    at: state.at,
    hits: state.hits,
    thresholdS: config.EXHAUSTION_GLOBAL_S,
  }
}

/** Test seam: forget the breaker so one process can run several scenarios. */
export function reset () {
  state.until = 0
  state.retryAfterS = 0
  state.model = null
  state.at = null
  state.hits = 0
}
