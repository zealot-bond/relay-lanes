// Public model catalogue for the relay.
//
// Public ids are namespaced "github/<model>" -- the backend vendor never appears
// in the client-facing API. The upstream id is internal only.
//
// Protocol note: two of these models are served by an OpenAI-style /responses
// endpoint that REJECTS Chat Completions fields and refuses non-streaming
// requests. A client posting to /v1/chat/completions must therefore be
// translated before it reaches upstream. protocolFor() is the single source of
// truth for that decision.

export const PROVIDER = 'github'

const RESPONSES_MODELS = new Set([
  'muse-spark-1.2-contributor-free',
  'muse-spark-1.3-contributor-free',
])

// Availability was measured directly against the gateway, one model per dialect,
// rather than taken from its published catalogue -- which is not a reliable health
// signal. Two proofs: mimo-v2.5-free is still LISTED there but answers
// HTTP 410 ModelDeprecated on every request, and jev-1.13-free is listed but
// answers 500 on both dialects.
//
// chat / responses outcome per candidate:
//
//   PASS chat     exo-free                      2.2s
//   PASS chat     space-bunny-free              0.8s
//   PASS chat     longcat-2.5-preview-free     4.6s
//   PASS chat     fledge-alpha-free             1.9s
//   PASS chat     ling-3.1-flash-free           1.4s
//   PASS chat     big-pickle                    4.3s
//   PASS chat     nemotron-3-ultra-free         5.1s
//   PASS chat     nemotron-3.5-lightning-free 18.3s
//   PASS chat     mimo-v2.6-flash-free          2.0s
//   PASS responses muse-spark-1.2-contributor-free 4.5s
//   PASS responses muse-spark-1.3-contributor-free 1.8s
//
//   FAIL chat 400 "Endpoint is unavailable"    ling-3.0-flash-fin-free
//   FAIL chat 400 "Model is unavailable"       deepseek-v4-flash-free
//   FAIL chat 500 / responses 500              jev-1.13-free
//   FAIL chat 410 "ModelDeprecated"            mimo-v2.5-free
//
// Every addition was probed on BOTH dialects. The five new models answer 500 or
// 401 on /v1/responses, so they are chat-only; mislabelling them would surface as
// `input[N] did not match any supported type` at request time rather than as a
// clear failure here.
//
// contextWindow is unverified for every entry -- the gateway's listing carries no
// metadata, and big-pickle has been observed serving 852k tokens despite the
// 200000 declared here. Treat it as a floor, not a limit.
export const MODELS = [
  { id: 'big-pickle', contextWindow: 200000 },
  { id: 'nemotron-3-ultra-free', contextWindow: 200000 },
  { id: 'nemotron-3.5-lightning-free', contextWindow: 200000 },
  { id: 'mimo-v2.6-flash-free', contextWindow: 200000 },
  { id: 'muse-spark-1.2-contributor-free', contextWindow: 200000 },
  { id: 'muse-spark-1.3-contributor-free', contextWindow: 200000 },
  // Verified present and answering on the chat dialect only.
  { id: 'exo-free', contextWindow: 200000 },
  { id: 'fledge-alpha-free', contextWindow: 200000 },
  { id: 'ling-3.1-flash-free', contextWindow: 200000 },
  { id: 'longcat-2.5-preview-free', contextWindow: 200000 },
  { id: 'space-bunny-free', contextWindow: 200000 },
]

export function protocolFor (modelId) {
  return RESPONSES_MODELS.has(modelId) ? 'responses' : 'chat'
}

/** Accept "github/x", bare "x", or the internal upstream id. */
export function splitModelId (id) {
  if (typeof id !== 'string' || !id.trim()) return { provider: PROVIDER, model: '' }
  const trimmed = id.trim()
  const idx = trimmed.indexOf('/')
  if (idx < 0) return { provider: PROVIDER, model: trimmed }
  return { provider: trimmed.slice(0, idx), model: trimmed.slice(idx + 1).trim() }
}

export function findModel (modelId) {
  const { model } = splitModelId(modelId)
  return MODELS.find((m) => m.id === model) || null
}

export function listModelsPayload (build) {
  const meta = build ? { build } : {}
  return {
    object: 'list',
    ...meta,
    data: MODELS.map((m) => ({
      id: `${PROVIDER}/${m.id}`,
      object: 'model',
      created: 0,
      owned_by: PROVIDER,
      context_window: m.contextWindow,
    })),
  }
}