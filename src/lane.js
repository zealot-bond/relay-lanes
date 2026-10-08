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
import { StringDecoder } from 'node:string_decoder'
import {
  zenBase, CHAT_PATH, RESPONSES_PATH,
  fingerprintToolSpecs, fingerprintToolSpecsResponses,
  newSessionId, newRequestId, headers, classifyUpstream,
} from './fingerprint.js'
import { protocolFor } from './models.js'
import { config } from './config.js'

/**
 * Upstream timeouts, split by phase.
 *
 * A single inactivity timeout cannot serve both phases. Measured: ~225k input
 * tokens -> 6s TTFT, ~567k -> 7-14s, ~852k -> 237s. The large-input cost is
 * prefill, during which the socket is silent but healthy. With one 240s
 * inactivity cap, a big-but-successful prefill was killed at 240s and retried, so
 * TTFT arrived as 240s x attempts -- the clean 4-minute figures users reported.
 *
 * So: a generous budget until the FIRST byte (prefill is allowed to be slow),
 * then a tight inactivity budget, because once tokens are flowing a stall is
 * genuinely wedged.
 */
const FIRST_BYTE_TIMEOUT_MS = config.FIRST_BYTE_TIMEOUT_MS
const STREAM_IDLE_TIMEOUT_MS = config.STREAM_IDLE_TIMEOUT_MS

const KEEPALIVE = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 30000,
  // A lane serves ONE upstream request at a time, so a pool of 16 sockets is not
  // about parallelism -- it is headroom so a socket left in TIME_WAIT is never a
  // reason to build a new TLS connection. maxFreeSockets is kept low because an
  // idle socket is dropped by the upstream gateway anyway; holding 8 dead ones
  // meant the first request after a quiet period paid a full handshake.
  maxSockets: 8,
  maxFreeSockets: 2,
  // Must outlast the first-byte budget, otherwise the agent kills the socket
  // before prefill completes.
  timeout: FIRST_BYTE_TIMEOUT_MS + 60000,
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
  // responses-dialect calls are keyed by output_index, which is NOT the array
  // position: a reasoning item takes a slot of its own. Map it to the call's own
  // real index so an interleaved or sparse stream still folds into one entry per
  // call with the right arguments attached.
  const callIndexByOutput = new Map()
  let usage = null
  let finishReason = null
  let sawDone = false
  // Accumulates the pieces of a multi-line `data:` field until it parses.
  let dataParts = []

  // Real indices for chat-dialect calls that arrive without an explicit index.
  const chatCallIndexById = new Map()
  let lastChatCallIndex = 0
  const resolveChatIndex = (tc, curLen) => {
    if (typeof tc.index === 'number') return tc.index
    if (tc.id) {
      if (chatCallIndexById.has(tc.id)) return chatCallIndexById.get(tc.id)
      const at = curLen
      chatCallIndexById.set(tc.id, at)
      lastChatCallIndex = at
      return at
    }
    return lastChatCallIndex
  }

  for (const line of raw.split(/\n/)) {
    // A blank line is the SSE event boundary. Whatever was accumulated and still does
    // not parse is a dead frame (a keepalive, a `data: ping`): drop it HERE. Left in
    // place it was joined onto every later frame, none of which then parsed, so one
    // stray non-JSON line silently discarded the rest of the stream.
    if (line.trim() === '') { dataParts = []; continue }
    if (!line.startsWith('data:')) continue
    // SSE allows a field to be split across several `data:` lines, which must be
    // joined with a newline before parsing. Reading each line independently made a
    // wrapped frame fail JSON.parse and get dropped -- which surfaced as an empty
    // answer that then retried forever while the client never saw the text.
    const piece = line.slice(5).replace(/^ /, '')
    if (!piece) continue
    if (sawDone) continue
    if (piece.trim() === '[DONE]') { sawDone = true; dataParts = []; continue }
    dataParts.push(piece)
    // A frame is complete when the next line is not a continuation. Blank-line
    // framing is handled by the outer split, so flush here when the JSON parses.
    const payload = dataParts.join('\n')
    let ev
    try { ev = JSON.parse(payload) } catch { continue }
    dataParts = []

    const delta = ev?.choices?.[0]?.delta
    const cd = delta?.content
    if (typeof cd === 'string') { parts.push(cd); onText?.(cd) }
    // Tool calls arrive as deltas keyed by index, so they must be merged by index
    // rather than appended. They also carry the whole answer for models that
    // decide to call a tool instead of replying in text: dropping them made the
    // completion look empty even though upstream had produced a valid response.
    if (Array.isArray(delta?.tool_calls)) {
      for (const tc of delta.tool_calls) {
        const at = resolveChatIndex(tc, toolCalls.length)
        const cur = toolCalls[at] ||= {
          id: '', type: 'function',
          function: { name: '', arguments: '' },
        }
        if (tc.id) cur.id = tc.id
        if (tc.type) cur.type = tc.type
        // Both fields are appended here, and that is correct for the chat dialect:
        // upstream splits the name across chunks ("ed"+"it") exactly as it splits
        // arguments, so a client reassembling fragments gets the right name. The
        // batch boundary is handled in worker.js, which sends each field once.
        if (tc.function?.name) cur.function.name += tc.function.name
        if (tc.function?.arguments) cur.function.arguments += tc.function.arguments
      }
    }
    // The responses dialect reports tool activity differently. Keyed by
    // output_index rather than append order: interleaved calls would otherwise merge
    // their arguments into whichever entry happened to be last.
    if (ev?.type === 'response.output_item.added' && ev?.item?.type === 'function_call') {
      const at = toolCalls.length
      if (typeof ev.output_index === 'number') callIndexByOutput.set(ev.output_index, at)
      else callIndexByOutput.set(`pos:${at}`, at)
      // Name and id are complete in this event, so they are assigned, never
      // appended: a repeated item event would otherwise double the name.
      toolCalls[at] = {
        id: ev.item.id || '',
        type: 'function',
        function: { name: ev.item.name || '', arguments: '' },
      }
    }
    if (ev?.type === 'response.function_call_arguments.delta' && typeof ev.delta === 'string') {
      // Route by output_index. Using "the last call seen" attached arguments to
      // the wrong call whenever two ran in parallel, which is what produced a
      // malformed call with missing required properties.
      let at = -1
      if (typeof ev.output_index === 'number') at = callIndexByOutput.get(ev.output_index) ?? -1
      if (at < 0 && typeof ev.item_id === 'string') {
        at = toolCalls.findIndex((c) => c && c.id === ev.item_id)
      }
      if (at < 0) at = toolCalls.length - 1
      if (at >= 0 && toolCalls[at]) toolCalls[at].function.arguments += ev.delta
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
 * These are reasoning models: the budget is consumed by reasoning tokens before
 * any visible text appears. Measured on muse-spark-1.3, prompt asking ~200 words:
 *
 *   max_tokens=800      empty answer, 40s
 *   max_tokens=1200     empty answer, 43s
 *   max_tokens=2000     empty answer, 49s
 *   max_tokens=4000     215 words, 22s
 *   max_tokens=omitted  214 words, 14s
 *
 * A budget under roughly 4000 yields NOTHING rather than a short answer, which
 * surfaced as "no lane could serve this request (empty)" and was misdiagnosed as
 * upstream flakiness for hours. The floor is therefore 4096. A client that sets no
 * limit gets the much larger default, so nothing is imposed on it.
 */
const MIN_OUTPUT_TOKENS = config.MIN_OUTPUT_TOKENS
const DEFAULT_OUTPUT_TOKENS = config.DEFAULT_OUTPUT_TOKENS

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
        if (url) parts.push({ type: 'input_image', image_url: url })
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
  // Fallback call ids must be UNIQUE. Two parallel calls that arrive without ids
  // both used "call_0", so their function_call items and their results collapsed
  // into one call: the model then saw a single tool invocation with the wrong
  // output, which is a silent wrong answer rather than a visible error.
  //
  // They must also PAIR: a function_call and the output that answers it need the
  // same id. One shared counter gave the calls call_0/call_1 and the outputs
  // call_2/call_3, so no output matched any call. Anonymous ids are therefore handed
  // to the calls first and consumed in order by the outputs that follow them.
  let anonymous = 0
  const unmatched = []
  const nextAnonId = () => `call_${anonymous++}`
  for (const m of messages || []) {
    const role = m?.role

    // Tool result -> function_call_output. This is the shape the API accepts.
    if (role === 'tool' || role === 'function') {
      const out = typeof m.content === 'string' ? m.content : JSON.stringify(m.content ?? '')
      input.push({
        type: 'function_call_output',
        call_id: m.tool_call_id || m.call_id || m.id || unmatched.shift() || nextAnonId(),
        output: out,
      })
      continue
    }

    // An assistant turn that invoked tools is a function_call item, not a message.
    if (role === 'assistant' && Array.isArray(m.tool_calls) && m.tool_calls.length) {
      for (const tc of m.tool_calls) {
        const anon = tc.id || tc.tool_call_id ? null : nextAnonId()
        if (anon) unmatched.push(anon)
        input.push({
          type: 'function_call',
          call_id: tc.id || tc.tool_call_id || anon,
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
  // An empty input is rejected by the endpoint, and it is reachable: a
  // conversation of empty turns folds to nothing. Sending one placeholder keeps
  // the failure a model answer rather than an opaque HTTP 400.
  if (!input.length) input.push({ role: 'user', content: [{ type: 'input_text', text: '(empty)' }] })
  return input
}

/**
 * Canonical form of a tool name for de-duplication.
 *
 * Harnesses capitalise: Claude-style clients send `Bash`, `Read`, `Edit`. The
 * fingerprint placeholders are lowercase (`bash`, `read`, `edit`). A
 * case-sensitive comparison therefore did not recognise the client's `Bash` as
 * the same tool, so the relay advertised SEVEN tools for a three-tool client:
 * the client's real schemas alongside placeholders with `properties: {}`. The
 * model was offered two bash tools, one accepting nothing, and tool calls landed
 * on whichever it picked.
 */
function normToolName (n) {
  return String(n || '').toLowerCase().replace(/[^a-z0-9]/g, '')
}

/** Tool name from either dialect: chat nests it, responses flattens it. */
function nameOfTool (t) {
  return t?.function?.name || t?.name || null
}

/**
 * Reduce a tool spec of ANY dialect to {name, description, parameters}.
 *
 * Harnesses send tool schemas in three different shapes and all of them reach this
 * relay, because clients post whatever their own SDK expects:
 *
 *   chat       { type:'function', function:{ name, description, parameters } }
 *   responses  { type:'function', name, description, parameters }
 *   anthropic  { name, description, input_schema }
 *
 * Previously anything without `t.function` was forwarded untouched. An
 * Anthropic-style spec therefore reached upstream as
 * `{name:'edit', input_schema:{...}}` -- no `type`, and crucially no
 * `parameters`. The model was never told that file_path, old_string and
 * new_string were required, so it emitted a call without them and the harness
 * reported:
 *
 *   invalid arguments: missing required property "file_path"
 *
 * The same passthrough also broke a responses-shaped spec sent to a chat model,
 * because chat nests the definition and responses flattens it.
 */
function toolShape (t) {
  const fn = t?.function || {}
  const parameters = t?.parameters || fn.parameters || t?.input_schema || t?.inputSchema ||
    t?.schema || null
  return {
    name: t?.name || fn.name || null,
    description: t?.description || fn.description || '',
    parameters: parameters && typeof parameters === 'object'
      ? parameters
      : { type: 'object', properties: {} },
  }
}

/** Chat completions shape: the definition lives under `function`. */
function toChatTool (t) {
  const { name, description, parameters } = toolShape(t)
  return { type: 'function', function: { name, description, parameters } }
}

/** Responses shape: the definition is flat at the top level. */
function toResponsesTool (t) {
  const { name, description, parameters } = toolShape(t)
  return { type: 'function', name, description, parameters }
}

/** Drop only specs with no name at all -- nothing upstream can address them. */
function usableTool (t) {
  return Boolean(toolShape(t).name)
}

/**
 * Chat-completions tool specs -> responses tool specs.
 *
 * The two dialects nest the function definition differently, so a client's specs
 * are renormalised rather than forwarded: whatever shape arrived, the model must
 * still be told what each tool's required arguments are.
 */
function toResponsesTools (tools) {
  return tools.filter(usableTool).map(toResponsesTool)
}

function buildUpstreamBody ({ model, messages, system, maxTokens, temperature, topP, tools, toolChoice, parallelToolCalls }) {
  const protocol = protocolFor(model)

  // Client tool specs are normalised into the target dialect's shape and unioned
  // with the gateway's own tools; they are never substituted for them.
  //
  // The gateway's access check needs its four tool names present EXACTLY as spelled
  // (bash, glob, grep, read). A gateway tool is therefore suppressed only when the
  // client declares a tool with that identical name. An earlier version matched
  // case-insensitively, so a client declaring `Bash` removed the gateway's `bash`,
  // and every request answered `403 FreeTierError` -- measured on big-pickle,
  // exo-free and muse-spark with an Anthropic-style Bash/Edit tool set, while the
  // same tools spelled lowercase passed.
  const clientTools = (Array.isArray(tools) ? tools : []).filter(usableTool)
  const hasClientTools = clientTools.length > 0
  const disableTools = toolChoice === 'none' || !hasClientTools
  const clientNames = new Set(clientTools.map(nameOfTool).filter(Boolean))

  const chatTools = [
    ...fingerprintToolSpecs().filter((t) => !clientNames.has(t.function.name)),
    ...clientTools.map(toChatTool),
  ]
  const respTools = [
    ...fingerprintToolSpecsResponses().filter((t) => !clientNames.has(t.name)),
    ...toResponsesTools(clientTools),
  ]

  if (protocol === 'responses') {
    // When tools are not desired, responses endpoint refuses tool_choice: "none"
    // (only "auto" is supported). We instruct the reasoning model directly in system
    // so it does not invoke tools.
    const noToolsInstruction = disableTools
      ? 'Do not call any tools or functions. Answer directly in text.'
      : null
    const effectiveSystem = [system, noToolsInstruction].filter(Boolean).join('\n\n')

    // `system` is folded in as a leading message so ordering is preserved.
    const merged = effectiveSystem
      ? [{ role: 'system', content: effectiveSystem }, ...(messages || [])]
      : (messages || [])
    const body = {
      model,
      input: toResponsesInput(merged),
      stream: true,                          // the responses endpoint requires it
      tools: respTools,
      max_output_tokens: outputBudget(maxTokens),
      ...(temperature !== undefined ? { temperature } : {}),
      ...(topP !== undefined ? { top_p: topP } : {}),
    }
    if (toolChoice && toolChoice !== 'none') body.tool_choice = toolChoice
    if (parallelToolCalls !== undefined) body.parallel_tool_calls = parallelToolCalls
    return body
  }

  // Chat dialect: keep the original messages, only normalise tool entries.
  const msgs = []
  let anonIds = 0
  if (system) msgs.push({ role: 'system', content: system })
  for (const m of messages || []) {
    if (m.role === 'tool') {
      msgs.push({
        // Distinct fallback ids, not a shared literal. A turn carrying several
        // tool results without tool_call_id gave them all `call_0`, so the first
        // was answered N times and the rest were orphaned. Matching the assistant
        // tool_calls order is the only reliable correlation available here.
        role: 'tool',
        tool_call_id: m.tool_call_id || `call_${anonIds++}`,
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
    tools: chatTools,
    max_tokens: outputBudget(maxTokens),
  }
  if (disableTools) {
    body.tool_choice = 'none'
  } else if (toolChoice) {
    body.tool_choice = toolChoice
  }
  if (parallelToolCalls !== undefined) body.parallel_tool_calls = parallelToolCalls
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
async function callUpstreamOnce (job, { signal, onDelta, onToolCall, onToolDelta } = {}) {
  const { model, messages, system, maxTokens, temperature, topP, tools, toolChoice, parallelToolCalls } = job || {}
  const protocol = protocolFor(model)
  const payload = JSON.stringify(buildUpstreamBody({ model, messages, system, maxTokens, temperature, topP, tools, toolChoice, parallelToolCalls }))
  const url = new URL(zenBase() + (protocol === 'responses' ? RESPONSES_PATH : CHAT_PATH))

  const started = Date.now()
  return new Promise((resolve) => {
    let settled = false
    let sawByte = false
    let idleTimer = null
    const clearIdle = () => { if (idleTimer) { clearTimeout(idleTimer); idleTimer = null } }
    const fin = (v) => {
      if (settled) return
      settled = true
      clearIdle()
      resolve(v)
    }

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
      // Decode through a StringDecoder, not chunk.toString(): a multi-byte character
      // (any CJK text, any emoji) can straddle two TCP reads, and decoding each read
      // on its own turned the split character into U+FFFD. Measured: the client-bound
      // stream carried "日���語" while the folded result was the correct "日本語".
      const decoder = new StringDecoder('utf8')
      // Tracks which output_index is currently receiving arguments, and the last
      // index seen, so the responses dialect can assign real positions.
      let activeCall = -1
      let nextFoldedPos = 0
      const callsByIndex = new Map()
      const outIdxToCall = new Map()
      const chatCallIndexById = new Map()
      let lastChatCallIndex = 0
      let chatCallCount = 0
      const resolveChatIndex = (d) => {
        if (typeof d.index === 'number') return d.index
        if (d.id) {
          if (chatCallIndexById.has(d.id)) return chatCallIndexById.get(d.id)
          const at = chatCallCount++
          chatCallIndexById.set(d.id, at)
          lastChatCallIndex = at
          return at
        }
        return lastChatCallIndex
      }
      res.on('data', (c) => {
        // Every byte is progress. The first one spends the long prefill budget;
        // from then on a silent socket is a stall rather than legitimate prefill.
        sawByte = true
        armIdle(STREAM_IDLE_TIMEOUT_MS)
        chunks.push(c)
        if (!onDelta && !onToolCall && !onToolDelta) return
        carry += decoder.write(c)
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
          if (Array.isArray(tc) && tc.length) {
            onToolCall?.()
            // Forwarded verbatim: an agent harness reassembles tool_calls from these
            // deltas, and dropping them is why a harness saw the model's intent
            // text and then nothing at all.
            onToolDelta?.(tc.map((d) => ({
              index: resolveChatIndex(d),
              ...(d.id ? { id: d.id } : {}),
              ...(d.type ? { type: d.type } : {}),
              ...(d.function ? { function: d.function } : {}),
            })))
          }
          if (ev?.type === 'response.output_item.added' && ev?.item?.type === 'function_call') {
            onToolCall?.()
            // The responses dialect reports tools differently; translate to the chat
            // shape so a chat client sees one consistent format.
            //
            // output_index is the real position. Hardcoding 0 -- as this did -- made
            // every parallel call collide on one index, so a client reassembling by
            // index merged separate calls' arguments into one malformed object.
            // Use the FOLDED array position, not output_index. parseUpstream places a call at
            // toolCalls[length], and the relay's finaliser keys streamed arguments
            // by that same position. Emitting output_index here made the two
            // disagree whenever a reasoning item occupied an earlier output slot
            // -- calls merged, ids landed on the wrong entry, and arguments became
            // unparseable.
            activeCall = nextFoldedPos++
            if (typeof ev.output_index === 'number') outIdxToCall.set(ev.output_index, activeCall)
            callsByIndex.set(activeCall, { index: activeCall, id: ev.item.id || '', type: 'function', function: { name: ev.item.name || '', arguments: '' } })
            // Emit ONLY the identity here. A shallow spread shares the nested
            // `function` object, so the arguments accumulated below would also
            // appear on this fragment -- and since the worker serialises fragments
            // at flush time (DELTA_BATCH_MS later), it would carry the *whole*
            // argument string and then be concatenated a second time by the
            // batch merger. That produced unparseable arguments and the client-side
            // "missing required property file_path" failure.
            onToolDelta?.([{
              index: activeCall,
              id: callsByIndex.get(activeCall).id,
              type: 'function',
              function: { name: callsByIndex.get(activeCall).function.name, arguments: '' },
            }])
          }
          if (ev?.type === 'response.function_call_arguments.delta' && typeof ev.delta === 'string') {
            // Route by output_index, falling back to item_id. "The last call seen"
            // attached arguments to the wrong call whenever two ran in parallel.
            let at = activeCall
            if (typeof ev.output_index === 'number' && outIdxToCall.has(ev.output_index)) {
              at = outIdxToCall.get(ev.output_index)
            } else if (typeof ev.item_id === 'string') {
              for (const [k, v] of callsByIndex) { if (v.id && v.id === ev.item_id) { at = k; break } }
            }
            const cur = callsByIndex.get(at)
            if (cur) {
              cur.function.arguments += ev.delta
              onToolDelta?.([{ index: cur.index, function: { arguments: ev.delta } }])
            }
          }

          const text = ev?.choices?.[0]?.delta?.content ??
            (ev?.type === 'response.output_text.delta' ? ev.delta : undefined)
          if (typeof text === 'string' && text) onDelta?.(text)
        }
      })
      // A response that is already flowing does not raise an error on the REQUEST when
      // the socket dies, so without these the call sat on a dead connection until the
      // idle timer fired -- two minutes at the production setting -- before it could be
      // retried. Observed: upstream sent one frame and reset; the call resolved only
      // after the whole idle budget, as a timeout.
      const abortedMidStream = (why) => fin({
        kind: 'error', status: res.statusCode || 0,
        raw: `upstream connection ${why} mid-stream`, data: null, ms: Date.now() - started,
      })
      res.on('aborted', () => abortedMidStream('aborted'))
      res.on('error', (e) => abortedMidStream(`failed (${e.code || e.message})`))
      res.on('close', () => { if (!res.complete) abortedMidStream('closed') })
      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString()
        const verdict = classifyUpstream(res.statusCode, raw)
        const data = verdict.kind === 'ok' ? parseUpstream(raw, model) : null
        const msg = data?.choices?.[0]?.message
        // A 200 that carries neither text nor tool calls is upstream flakiness,
        // not an answer. Measured on muse-spark: 4 of 5 requests. Reported as
        // 'empty' so the relay retries on another egress IP instead of handing
        // the client a blank completion.
        const hasChoices = Array.isArray(data?.choices) && data.choices.length > 0
        // A 200 whose body is an error envelope (no `choices`) is not a successful
        // empty answer: serving it gave the client content:"" with finish_reason
        // "stop", HTTP 200, no retry and no error. Such a body is reported as
        // provider_error so it is counted, retried once, and surfaced honestly.
        const errorInOk = verdict.kind === 'ok' && data && !hasChoices && data.error
        const hasToolCalls = Array.isArray(msg?.tool_calls) && msg.tool_calls.length > 0
        const isEmpty = verdict.kind === 'ok' && hasChoices &&
          !String(msg?.content ?? '').trim() &&
          !hasToolCalls
        fin({
          ...verdict,
          ...(errorInOk ? { kind: 'provider_error' } : {}),
          ...(isEmpty ? { kind: 'empty' } : {}),
          status: res.statusCode,
          raw,
          data,
          ms: Date.now() - started,
        })
      })
    })

    // Phase 1: waiting on the first byte. Prefill on a large prompt is slow but
    // healthy, so the budget is long. Phase 2: after tokens start, a silent
    // socket means something is wedged, so the budget tightens sharply.
    //
    // The timer is re-armed WITH a callback every time. `setTimeout(ms, cb)` on a
    // request registers a one-shot 'timeout' listener, so re-arming without one
    // (as this did) left the second window with no handler at all: a connection
    // that never sent a byte hung the lane until the job's own timeout instead of
    // being abandoned and retried on another runner.
    const onIdle = () => {
      idleTimer = null
      // Both phases end the same way; only the budget differs. A stall with the
      // wrong wall-clock label is still a stall, and the attempt is retried.
      req.destroy()
      fin({
        kind: 'timeout',
        status: 0,
        raw: sawByte ? 'stream stalled after first byte' : 'no first byte within budget',
        data: null,
        ms: Date.now() - started,
      })
    }
    const armIdle = (ms) => {
      clearIdle()
      // A local timer is used rather than req.setTimeout because it can be
      // re-armed indefinitely and fires exactly once per window. The socket-level
      // timeout is left to the agent, which must outlast the prefill budget.
      idleTimer = setTimeout(onIdle, ms)
      idleTimer.unref?.()
    }
    armIdle(FIRST_BYTE_TIMEOUT_MS)
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
// Default is passthrough: the caller is an agent harness that runs its own tools,
// so a tool_call must reach it untouched. 'self' (answering tool calls with prose
// and forbidding them in a preamble) is wrong for a harness -- it turned every
// "I'll read the configs" into narration followed by nothing, which is exactly the
// "fake promise" symptom.
const TOOL_MODE = String(config.TOOL_MODE || 'passthrough').toLowerCase()
/**
 * Decide what to do with a tool call, given what the client has already seen.
 *
 * The three tempting behaviours are mutually exclusive once SSE is involved:
 *
 *   buffer then retract  retraction cannot unsend what already streamed, so a
 *                        late call left partial text plus a second answer. That
 *                        produced literal duplicates ("...help with it.I can't
 *                        read README.md directly") and mid-sentence interleaving
 *                        ("so I can't access you paste the contents of ` it").
 *
 *   re-ask unconditionally that duplication, every time.
 *
 *   never re-ask            true streaming, but a client that cannot execute
 *                        tools gets a promise and nothing else.
 *
 * So: self-answer only when no text has been forwarded yet, which is the common
 * case for a pure tool call. Once text is out, keep it and stop. The result is
 * coherent output in every case, at the cost of not retrying a text+call turn --
 * and the preamble makes that turn rare in the first place.
 */
const TOOL_UNAVAILABLE =
  'Unavailable. This session has no execution environment: the bash, read, grep ' +
  'and glob tools are declared only to satisfy the API contract and cannot be ' +
  'run. Do not claim to have used them. Answer now using only what is in the ' +
  'conversation, and say plainly what you cannot do.'

export async function callUpstream (job, opts = {}) {
  const clientTools = (Array.isArray(job?.tools) ? job.tools : []).filter(usableTool)
  const isPlainChat = clientTools.length === 0 || job?.toolChoice === 'none'
  const effectiveMode = isPlainChat ? 'self' : TOOL_MODE
  if (effectiveMode === 'passthrough') return callUpstreamOnce(job, opts)

  // The self-answer loop must run for non-streaming clients too. Returning early
  // when there is no onDelta left those clients with the raw promise plus a
  // dangling tool call, which is the original bug for anyone not using SSE.
  // Retraction is only needed when there is a client to retract from, so a
  // no-op forward is correct and harmless.
  const onDelta = typeof opts.onDelta === 'function' ? opts.onDelta : () => {}
  const onToolCall = typeof opts.onToolCall === 'function' ? opts.onToolCall : () => {}

  // One round: forward text as it arrives and remember whether any was sent.
  const runRound = (roundJob) => {
    let sentText = false
    return callUpstreamOnce(roundJob, {
      ...opts,
      onDelta: (t) => { sentText = true; onDelta(t) },
      onToolCall: () => {
        onToolCall()
        opts.onToolCall?.()
      },
    }).then((r) => ({ r, sentText }))
  }

  let { r: result, sentText } = await runRound(job)
  if (effectiveMode !== 'self' || result.kind !== 'ok') return result

  const toolCallsOf = (r) => r.data?.choices?.[0]?.message?.tool_calls

  // Text already delivered and a call alongside it: keep the text. Retracting is
  // impossible in SSE and re-asking would duplicate it.
  let calls = toolCallsOf(result)
  if (!Array.isArray(calls) || calls.length === 0) return result
  if (sentText) return result

  const history = []
  if (job?.system) history.push({ role: 'system', content: job.system })
  for (const m of job?.messages || []) history.push(m)

  const TOOL_ROUNDS = config.TOOL_ROUNDS
  for (let round = 1; round <= TOOL_ROUNDS; round++) {
    history.push({ role: 'assistant', content: null, tool_calls: calls })
    for (const tc of calls) {
      history.push({ role: 'tool', tool_call_id: tc.id || 'call_0', content: TOOL_UNAVAILABLE })
    }
    const out = await runRound({ ...job, messages: history })
    if (out.r.kind !== 'ok') return result
    result = out.r
    calls = toolCallsOf(result)
    if (!Array.isArray(calls) || calls.length === 0) return result
    // This round spoke before calling a tool: stop here and keep its text.
    if (out.sentText) return result
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