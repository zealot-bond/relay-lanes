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

/**
 * Per-request upstream timeout.
 *
 * This is a socket INACTIVITY timeout, not a total budget: Node resets it on
 * every byte received. That distinction matters because these are reasoning
 * models -- they can emit nothing for 30-60s while thinking, and the measured
 * cost of the old 45s cap was that it fired during exactly that gap and killed
 * long generations mid-answer. 412 timeouts against 2308 served on the panel.
 *
 * A stream that keeps producing tokens can therefore run for as long as it likes;
 * only a genuinely wedged connection is abandoned.
 */
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 240000)

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

/**
 * Normalise one message's content for the /v1/responses `input` array.
 *
 * The responses API is strict: a content part must be input_text or output_text,
 * chosen by role, and a bare `String(content)` turns a client's content-parts
 * array into the literal "[object Object]", which is how a multimodal turn became
 * an unusable request. Returns null when there is nothing worth sending, so an
 * empty assistant turn is dropped rather than sent as empty output_text.
 */
function responsesContent (content, role) {
  const partType = role === 'assistant' ? 'output_text' : 'input_text'
  if (content === null || content === undefined) return null

  if (typeof content === 'string') {
    return content.length ? [{ type: partType, text: content }] : null
  }

  if (Array.isArray(content)) {
    const parts = []
    for (const p of content) {
      if (typeof p === 'string') {
        if (p) parts.push({ type: partType, text: p })
        continue
      }
      if (!p || typeof p !== 'object') continue
      // Preserve images and other real parts; only rewrite text shape.
      if (p.type === 'text' || p.type === 'input_text' || p.type === 'output_text') {
        if (p.text) parts.push({ type: partType, text: p.text })
        continue
      }
      if (p.type === 'image_url' || p.type === 'image') {
        const url = p.image_url?.url ?? p.url ?? p.image_url
        if (url) parts.push({ type: 'input_image', image_url: typeof url === 'string' ? url : url })
        continue
      }
      if (p.type) { parts.push({ ...p, type: partType === 'output_text' ? 'output_text' : p.type }); continue }
    }
    return parts.length ? parts : null
  }

  if (typeof content === 'object') {
    if (content.text) return [{ type: partType, text: content.text }]
    return null
  }
  const s = String(content)
  return s.length ? [{ type: partType, text: s }] : null
}

/**
 * Translate OpenAI chat messages into the responses `input` array.
 *
 * This is the part that was wrong. Every message was emitted as
 * `{type:'message', role:<role>, content:[...]}` -- including tool results. The
 * responses API has no `role:'tool'` message: a tool result is a separate
 * function_call_output item keyed by call_id, and an assistant turn that called a
 * tool is a function_call item. Sending the message form produced
 *
 *   HTTP 400  `input[8]` did not match any supported type
 *
 * which surfaced to clients as "model is not answering upstream" and, because the
 * position depends on conversation length, only failed once a conversation was
 * long enough to contain a tool result -- so short tests always passed.
 */
function toResponsesInput (messages) {
  const input = []
  for (const m of messages || []) {
    const role = m?.role

    // Tool result -> function_call_output. This is the shape the API accepts.
    if (role === 'tool' || role === 'function') {
      const out = typeof m.content === 'string' ? m.content : JSON.stringify(m.content ?? '')
      input.push({
        type: 'function_call_output',
        call_id: m.tool_call_id || m.call_id || m.id || 'call_0',
        output: out,
      })
      continue
    }

    // An assistant turn that invoked tools is a function_call item, not a message.
    if (role === 'assistant' && Array.isArray(m.tool_calls) && m.tool_calls.length) {
      for (const tc of m.tool_calls) {
        input.push({
          type: 'function_call',
          call_id: tc.id || tc.tool_call_id || 'call_0',
          name: tc.function?.name || tc.name || '',
          arguments: typeof tc.function?.arguments === 'string'
            ? tc.function.arguments
            : JSON.stringify(tc.function?.arguments ?? {}),
        })
      }
      // A turn can carry both a tool call and text; keep the text as a message.
    }

    const responsesRole = role === 'assistant' ? 'assistant'
      : (role === 'system' || role === 'developer') ? role
        : 'user'

    const content = responsesContent(m?.content, responsesRole)
    if (content) input.push({ role: responsesRole, content })
  }
  return input
}

function buildUpstreamBody ({ model, messages, system, maxTokens, temperature, topP }) {
  const protocol = protocolFor(model)

  if (protocol === 'responses') {
    // `system` is folded in as a leading message so ordering is preserved.
    const merged = system
      ? [{ role: 'system', content: system }, ...(messages || [])]
      : (messages || [])
    return {
      model,
      input: toResponsesInput(merged),
      stream: true,                          // the responses endpoint requires it
      tools: fingerprintToolSpecsResponses(),
      max_output_tokens: outputBudget(maxTokens),
      ...(temperature !== undefined ? { temperature } : {}),
      ...(topP !== undefined ? { top_p: topP } : {}),
    }
  }

  // Chat dialect: keep the original messages, only normalise tool entries.
  const msgs = []
  if (system) msgs.push({ role: 'system', content: system })
  for (const m of messages || []) {
    if (m.role === 'tool') {
      msgs.push({
        role: 'tool',
        tool_call_id: m.tool_call_id || 'call_0',
        content: typeof m.content === 'string' ? m.content : JSON.stringify(m.content ?? ''),
      })
    } else if (m.role === 'assistant' && Array.isArray(m.tool_calls) && m.tool_calls.length) {
      msgs.push({ role: 'assistant', content: m.content ?? null, tool_calls: m.tool_calls })
    } else {
      msgs.push({ role: m.role, content: m.content ?? '' })
    }
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
 *
 * This is the single-shot form. callUpstream() wraps it to handle tool calls.
 */
async function callUpstreamOnce (job, { signal, onDelta, onToolCall } = {}) {
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
        if (!onDelta && !onToolCall) return
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
          // Announce tool calls as they arrive, not at 'end'. The caller needs to
          // know mid-stream so it can withhold the text that precedes the call --
          // otherwise the client is shown "I'll read that file" and only then
          // finds out nothing will run it.
          const tc = ev?.choices?.[0]?.delta?.tool_calls
          if (Array.isArray(tc) && tc.length) onToolCall?.()
          if (ev?.type === 'response.output_item.added' && ev?.item?.type === 'function_call') onToolCall?.()

          const text = ev?.choices?.[0]?.delta?.content ??
            (ev?.type === 'response.output_text.delta' ? ev.delta : undefined)
          if (typeof text === 'string' && text) onDelta?.(text)
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

/**
 * Tool calls the relay cannot execute, answered on the client's behalf.
 *
 * The declared tools are not a convenience: removing them makes upstream answer
 * 403 "free tier can only be used from within OpenCode", so they ARE the access
 * gate and must stay. The cost is that the model sees bash/read/grep/glob, says
 * "I'll read the file", emits a call, and nothing ever runs it -- the reported
 * "makes fake promises, never actually does anything".
 *
 * So the relay closes the loop itself: the call is appended to the conversation
 * together with a result saying the tool is unavailable, and the model is asked
 * to reply for real. The client receives an actual answer instead of a dangling
 * promise it cannot fulfil.
 *
 * TOOL_MODE=passthrough restores the previous behaviour of forwarding tool_calls
 * to the client, which is correct when the client is an agent that can run them.
 */
const TOOL_MODE = (process.env.TOOL_MODE || 'self').toLowerCase()
const TOOL_ROUNDS = Number(process.env.TOOL_ROUNDS || 2)
// Text is held this long before being forwarded, so a tool call arriving in the
// same breath as the text can still cancel it. Negligible next to a multi-second
// time-to-first-token, and it is the only way to avoid shipping the client a
// promise the relay is about to retract.
const TOOL_TEXT_HOLD_MS = Number(process.env.TOOL_TEXT_HOLD_MS || 250)

/**
 * Per-round text sink that can retract.
 *
 * The model emits "I'll read the file" and a tool call in the same response. Text
 * forwarded eagerly is already on the client's screen when the call turns up, and
 * SSE gives no way to take it back -- which is why the earlier self-answer fix
 * changed nothing visible: the promise had already been streamed. So text is
 * buffered briefly; a tool call discards the buffer and the round is redone.
 */
function makeRoundSink (onDelta, holdMs) {
  let buf = []
  let timer = null
  let streaming = false
  let retracted = false

  const flush = () => {
    if (timer) { clearTimeout(timer); timer = null }
    const text = buf.join('')
    buf = []
    streaming = true
    if (text) onDelta(text)
  }

  return {
    push (text) {
      if (retracted || streaming) { if (!retracted) onDelta(text); return }
      buf.push(text)
      if (!timer) timer = setTimeout(flush, holdMs)
    },
    /** A tool call arrived: everything buffered for this round is discarded. */
    retract () {
      retracted = true
      if (timer) { clearTimeout(timer); timer = null }
      buf = []
    },
    /** Round finished: forward whatever is still held. */
    finish () {
      if (retracted) return
      if (!streaming) flush()
    },
  }
}
const TOOL_UNAVAILABLE =
  'Unavailable. This session has no execution environment: the bash, read, grep ' +
  'and glob tools are declared only to satisfy the API contract and cannot be ' +
  'run. Do not claim to have used them. Answer now using only what is in the ' +
  'conversation, and say plainly what you cannot do.'

export async function callUpstream (job, opts = {}) {
  if (TOOL_MODE === 'passthrough') return callUpstreamOnce(job, opts)

  // The self-answer loop must run for non-streaming clients too. Returning early
  // when there is no onDelta left those clients with the raw promise plus a
  // dangling tool call, which is the original bug for anyone not using SSE.
  // Retraction is only needed when there is a client to retract from, so a
  // no-op forward is correct and harmless.
  const onDelta = typeof opts.onDelta === 'function' ? opts.onDelta : () => {}

  const sink = makeRoundSink(onDelta, TOOL_TEXT_HOLD_MS)

  const runRound = () => callUpstreamOnce(job, {
    ...opts,
    onDelta: (t) => sink.push(t),
    onToolCall: () => sink.retract(),
  })

  let result = await runRound()
  sink.finish()
  if (TOOL_MODE !== 'self' || result.kind !== 'ok') return result

  let calls = result.data?.choices?.[0]?.message?.tool_calls
  if (!Array.isArray(calls) || calls.length === 0) return result

  const history = []
  if (job?.system) history.push({ role: 'system', content: job.system })
  for (const m of job?.messages || []) history.push(m)
  history.push({ role: 'assistant', content: null, tool_calls: calls })
  for (const tc of calls) {
    history.push({ role: 'tool', tool_call_id: tc.id || 'call_0', content: TOOL_UNAVAILABLE })
  }

  for (let round = 1; round <= TOOL_ROUNDS; round++) {
    // Each follow-up round gets a fresh sink: its text is the answer the client
    // should see, and a further tool call retracts it in turn.
    const nextSink = makeRoundSink(onDelta, TOOL_TEXT_HOLD_MS)
    const next = await callUpstreamOnce({ ...job, messages: history }, {
      ...opts,
      onDelta: (t) => nextSink.push(t),
      onToolCall: () => nextSink.retract(),
    })
    nextSink.finish()
    if (next.kind !== 'ok') return result
    result = next
    calls = result.data?.choices?.[0]?.message?.tool_calls
    if (!Array.isArray(calls) || calls.length === 0) return result
    history.push({ role: 'assistant', content: null, tool_calls: calls })
    for (const tc of calls) {
      history.push({ role: 'tool', tool_call_id: tc.id || 'call_0', content: TOOL_UNAVAILABLE })
    }
  }
  return result
}

export function destroyKeepalive () { KEEPALIVE.destroy() }

// Exposed for the diagnostic test, which needs to fold a captured stream without
// issuing a request. Same code path the lane uses, so a difference in behaviour
// cannot be hidden by a second implementation.
export const parseForTest = parseUpstream

// The converter is unit-tested offline against a tool-using conversation, which
// is how the `input[8]` shape bug was caught without needing a live request.
export const buildForTest = buildUpstreamBody