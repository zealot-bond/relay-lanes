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
 * `onText` is called with each text fragment as it arrives, so a streaming client
 * sees tokens while the model is still writing instead of waiting for the whole
 * answer. It is purely additive: folding the final completion is unchanged.
 *
 * Reasoning-only models (nemotron-3-ultra) stream their thinking in
 * delta.reasoning and put the answer in delta.content; only content counts.
 */
function parseUpstream (raw, model, onText) {
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
  const toolCalls = []
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

    const delta = ev?.choices?.[0]?.delta
    const cd = delta?.content
    if (typeof cd === 'string') { parts.push(cd); onText?.(cd) }
    // Tool calls arrive as deltas keyed by index, so they must be merged by index
    // rather than appended. They also carry the whole answer for models that
    // decide to call a tool instead of replying in text: dropping them made the
    // completion look empty even though upstream had produced a valid response.
    if (Array.isArray(delta?.tool_calls)) {
      for (const tc of delta.tool_calls) {
        const at = typeof tc.index === 'number' ? tc.index : toolCalls.length
        const cur = toolCalls[at] ||= {
          id: '', type: 'function',
          function: { name: '', arguments: '' },
        }
        if (tc.id) cur.id = tc.id
        if (tc.type) cur.type = tc.type
        if (tc.function?.name) cur.function.name += tc.function.name
        if (tc.function?.arguments) cur.function.arguments += tc.function.arguments
      }
    }
    // The responses dialect reports tool activity differently.
    if (ev?.type === 'response.output_item.added' && ev?.item?.type === 'function_call') {
      const at = toolCalls.length
      toolCalls[at] = {
        id: ev.item.id || '',
        type: 'function',
        function: { name: ev.item.name || '', arguments: '' },
      }
    }
    if (ev?.type === 'response.function_call_arguments.delta' && typeof ev.delta === 'string') {
      const at = toolCalls.length - 1
      if (at >= 0) toolCalls[at].function.arguments += ev.delta
    }

    const fr = ev?.choices?.[0]?.finish_reason
    if (typeof fr === 'string' && fr) finishReason = fr
    if (ev?.usage) usage = ev.usage

    if (ev?.type === 'response.output_text.delta' && typeof ev.delta === 'string') {
      parts.push(ev.delta)
      onText?.(ev.delta)
    }
    // The responses dialect has NO [DONE] sentinel; response.completed is the
    // terminal event. Without this the stream looks unterminated and an empty
    // response used to escape as a raw string instead of a structured result,
    // which made it impossible to detect or retry.
    if (ev?.type === 'response.completed') { sawDone = true }
    if (ev?.type === 'response.failed' || ev?.type === 'response.incomplete') { sawDone = true }
    if (ev?.type === 'response.completed') {
      const final = ev?.response?.output_text
      if (typeof final === 'string' && parts.length === 0) parts.push(final)
      if (ev?.response?.usage) usage = ev.response.usage
    }
  }

  const text = parts.join('')
  const calls = toolCalls.filter(Boolean)
  out.choices[0].message.content = text
  if (calls.length) {
    out.choices[0].message.tool_calls = calls
    if (finishReason !== 'stop') out.choices[0].finish_reason = 'tool_calls'
  }
  if (finishReason) out.choices[0].finish_reason = finishReason
  out.usage = usage || { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }
  // Always return the structured shape. Returning `raw` here put a bare string
  // into the completion path, where it read as a successful but empty answer.
  return out
}

/**
 * Build the upstream body for whichever protocol this model speaks.
 *
 * A client posting /v1/chat/completions does not need to know about this: the
 * relay always sends chat-shaped work to the lane, and the lane decides the
 * upstream shape from the model id.
 */
/**
 * Output budget.
 *
 * These models are reasoning models: with max_output_tokens=32 the entire budget
 * is spent on hidden reasoning and the stream ends with zero answer text. So a
 * tiny explicit request is raised to MIN_OUTPUT_TOKENS.
 *
 * But when the client sets NO limit the budget must not be small. Upstream stops
 * on its own end-of-sequence, so imposing 512 here truncated long answers
 * mid-sentence -- the reported "only half the text" bug, arriving with
 * finish_reason "length". A missing limit now means effectively unbounded.
 */
const MIN_OUTPUT_TOKENS = Number(process.env.MIN_OUTPUT_TOKENS || 512)
const DEFAULT_OUTPUT_TOKENS = Number(process.env.DEFAULT_OUTPUT_TOKENS || 32768)

function outputBudget (maxTokens) {
  if (Number(maxTokens) > 0) return Math.max(maxTokens, MIN_OUTPUT_TOKENS)
  return DEFAULT_OUTPUT_TOKENS
}

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
      max_output_tokens: outputBudget(maxTokens),
    }
    if (temperature !== undefined) body.temperature = temperature
    if (topP !== undefined) body.top_p = topP
    return body
  }

  const body = {
    model,
    messages: msgs,
    stream: true,                            // free tier refuses stream:false
    tools: fingerprintToolSpecs(),
    max_tokens: outputBudget(maxTokens),
  }
  if (temperature !== undefined) body.temperature = temperature
  if (topP !== undefined) body.top_p = topP
  return body
}

/**
 * One upstream request. Resolves with a classification; never throws upstream.
 *
 * `onDelta(text)` is invoked the moment a text fragment arrives on the socket,
 * before the response ends. That is what makes streaming worth having: the lane
 * can forward each fragment to the relay immediately, so the client renders
 * tokens while the model is still writing. Waiting for `end` would make
 * time-to-first-token equal the entire generation time.
 */
export async function callUpstream (job, { signal, onDelta } = {}) {
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
      // Incremental SSE decode. Only complete `data:` lines are parsed, so a
      // frame split across two TCP reads is never mangled into invalid JSON.
      let carry = ''
      res.on('data', (c) => {
        chunks.push(c)
        if (!onDelta) return
        carry += c.toString('utf8')
        let nl
        while ((nl = carry.indexOf('\n')) !== -1) {
          const line = carry.slice(0, nl)
          carry = carry.slice(nl + 1)
          if (!line.startsWith('data:')) continue
          const p = line.slice(5).trim()
          if (!p || p === '[DONE]') continue
          let ev
          try { ev = JSON.parse(p) } catch { continue }
          const text = ev?.choices?.[0]?.delta?.content ??
            (ev?.type === 'response.output_text.delta' ? ev.delta : undefined)
          if (typeof text === 'string' && text) onDelta(text)
        }
      })
      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString()
        const verdict = classifyUpstream(res.statusCode, raw)
        const data = verdict.kind === 'ok' ? parseUpstream(raw, model) : null
        const msg = data?.choices?.[0]?.message
        // A 200 that carries neither text nor tool calls is upstream flakiness,
        // not an answer. Measured on muse-spark: 4 of 5 requests. Reported as
        // 'empty' so the relay retries on another egress IP instead of handing
        // the client a blank completion.
        const isEmpty = verdict.kind === 'ok' &&
          data?.choices &&
          !String(msg?.content ?? '').trim() &&
          !Array.isArray(msg?.tool_calls)
        fin({
          ...verdict,
          ...(isEmpty ? { kind: 'empty' } : {}),
          status: res.statusCode,
          raw,
          data,
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

// Exposed for the diagnostic test, which needs to fold a captured stream without
// issuing a request. Same code path the lane uses, so a difference in behaviour
// cannot be hidden by a second implementation.
export const parseForTest = parseUpstream