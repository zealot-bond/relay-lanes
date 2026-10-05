// GitHub Actions lane worker.
//
// One job == one runner == one egress IP == one upstream rate-limit bucket.
// That is the entire reason this exists: measured on this account, 20 runners
// with 20 distinct Azure IPs served 1456 requests with zero 429s while the relay
// host's own egress was rate limited on every request.
//
// Latency, measured (run 37318129878): p50 ~1.5s, p95 15-45s. Two fixes applied:
//   - a keep-alive agent per process, so TLS/HTTP setup is paid once, not per
//     request (this was the largest avoidable cost)
//   - a hard per-request timeout, because a 45s request is useless interactively
//     and only blocks the lane's slot
//
// Lifecycle: claim -> serve -> report -> repeat. On a 429 the worker exits 75 so
// its burned IP is replaced by a fresh runner, which is what keeps the pool at a
// steady 20 without central scheduling.

import https from 'node:https'
import {
  ZEN_BASE, CHAT_PATH, RESPONSES_PATH,
  fingerprintToolSpecs, fingerprintToolSpecsResponses,
  newSessionId, newRequestId, headers, classifyUpstream,
} from './fingerprint.js'
import { protocolFor } from './models.js'

const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 45000)

const KEEPALIVE = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 30000,
  maxSockets: 16,
  maxFreeSockets: 8,
  timeout: REQUEST_TIMEOUT_MS,
})

/**
 * Fold an upstream SSE stream into one completion object.
 *
 * Two dialects arrive here and both must be handled:
 *   chat completions: data: {"choices":[{"delta":{"content":"..."}}]}
 *   responses:        data: {"type":"response.output_text.delta","delta":"..."}
 *
 * Text is appended ONLY from the field that actually carries it. An earlier
 * version appended both ev.delta and choices[0].delta.content for the same
 * frame, which double counted the output.
 *
 * Reasoning-only models (nemotron-3-ultra) stream their thinking in
 * delta.reasoning and put the answer in delta.content; only content counts.
 */
function parseUpstream (raw, model) {
  if (!raw) return null
  const trimmed = raw.trimStart()
  if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
    try { return JSON.parse(trimmed) } catch { /* fall through to SSE */ }
  }

  const out = {
    model,
    choices: [{ index: 0, message: { role: 'assistant', content: '' }, finish_reason: 'stop' }],
  }
  const parts = []
  let usage = null
  let finishReason = null
  let sawDone = false

  for (const line of raw.split(/\n/)) {
    if (!line.startsWith('data:')) continue
    const payload = line.slice(5).trim()
    if (!payload) continue
    if (payload === '[DONE]') { sawDone = true; continue }

    let ev
    try { ev = JSON.parse(payload) } catch { continue }

    const cd = ev?.choices?.[0]?.delta?.content
    if (typeof cd === 'string') parts.push(cd)
    const fr = ev?.choices?.[0]?.finish_reason
    if (typeof fr === 'string' && fr) finishReason = fr
    if (ev?.usage) usage = ev.usage

    if (ev?.type === 'response.output_text.delta' && typeof ev.delta === 'string') {
      parts.push(ev.delta)
    }
    if (ev?.type === 'response.completed') {
      const final = ev?.response?.output_text
      if (typeof final === 'string' && parts.length === 0) parts.push(final)
      if (ev?.response?.usage) usage = ev.response.usage
    }
  }

  const text = parts.join('')
  out.choices[0].message.content = text
  if (finishReason) out.choices[0].finish_reason = finishReason
  out.usage = usage || { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }
  if (!text && !usage) return sawDone ? out : raw
  return out
}

/**
 * Build the upstream body for whichever protocol this model speaks.
 *
 * A client posting /v1/chat/completions does not need to know about this: the
 * relay always sends chat-shaped work to the lane, and the lane decides the
 * upstream shape from the model id.
 */
function buildUpstreamBody ({ model, messages, system, maxTokens, temperature, topP }) {
  const protocol = protocolFor(model)
  const msgs = []
  if (system) msgs.push({ role: 'system', content: system })
  for (const m of messages || []) {
    if (m.role === 'tool') {
      msgs.push({ role: 'tool', tool_call_id: m.tool_call_id || 'call_0', content: String(m.content ?? '') })
    } else {
      msgs.push({ role: m.role, content: m.content ?? '' })
    }
  }

  if (protocol === 'responses') {
    const body = {
      model,
      input: msgs.map((m) => ({
        type: 'message',
        role: m.role,
        content: [{ type: m.role === 'assistant' ? 'output_text' : 'input_text', text: String(m.content ?? '') }],
      })),
      stream: true,                          // the responses endpoint requires it
      tools: fingerprintToolSpecsResponses(),
    }
    if (maxTokens && maxTokens > 0) body.max_output_tokens = maxTokens
    if (temperature !== undefined) body.temperature = temperature
    if (topP !== undefined) body.top_p = topP
    return body
  }

  const body = {
    model,
    messages: msgs,
    stream: true,                            // free tier refuses stream:false
    tools: fingerprintToolSpecs(),
  }
  if (maxTokens && maxTokens > 0) body.max_tokens = maxTokens
  if (temperature !== undefined) body.temperature = temperature
  if (topP !== undefined) body.top_p = topP
  return body
}

/** One upstream request. Resolves with a classification; never throws upstream. */
export async function callUpstream (job, { signal } = {}) {
  const { model, messages, system, maxTokens, temperature, topP } = job || {}
  const protocol = protocolFor(model)
  const payload = JSON.stringify(buildUpstreamBody({ model, messages, system, maxTokens, temperature, topP }))
  const url = new URL(ZEN_BASE + (protocol === 'responses' ? RESPONSES_PATH : CHAT_PATH))

  const started = Date.now()
  return new Promise((resolve) => {
    let settled = false
    const fin = (v) => { if (!settled) { settled = true; resolve(v) } }

    const req = https.request({
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname,
      method: 'POST',
      agent: KEEPALIVE,
      headers: {
        ...headers({ sessionId: newSessionId(), requestId: newRequestId(), protocol }),
        'Content-Length': Buffer.byteLength(payload),
      },
    }, (res) => {
      const chunks = []
      res.on('data', (c) => chunks.push(c))
      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString()
        const verdict = classifyUpstream(res.statusCode, raw)
        fin({
          ...verdict,
          status: res.statusCode,
          raw,
          data: verdict.kind === 'ok' ? parseUpstream(raw, model) : null,
          ms: Date.now() - started,
        })
      })
    })

    req.setTimeout(REQUEST_TIMEOUT_MS, () => {
      req.destroy()
      fin({ kind: 'timeout', status: 0, raw: '', data: null, ms: Date.now() - started })
    })
    req.on('error', (e) => fin({ kind: 'error', status: 0, raw: String(e.message), data: null, ms: Date.now() - started }))
    if (signal) {
      signal.addEventListener('abort', () => {
        req.destroy()
        fin({ kind: 'aborted', status: 0, raw: '', data: null, ms: Date.now() - started })
      })
    }
    req.write(payload)
    req.end()
  })
}

export function destroyKeepalive () { KEEPALIVE.destroy() }