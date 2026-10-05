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

export const MODELS = [
  { id: 'big-pickle', contextWindow: 200000 },
  { id: 'nemotron-3-ultra-free', contextWindow: 200000 },
  { id: 'nemotron-3.5-lightning-free', contextWindow: 200000 },
  { id: 'mimo-v2.5-free', contextWindow: 200000 },
  { id: 'mimo-v2.6-flash-free', contextWindow: 200000 },
  { id: 'ling-3.0-flash-fin-free', contextWindow: 200000 },
  { id: 'jev-1.13-free', contextWindow: 200000 },
  { id: 'muse-spark-1.2-contributor-free', contextWindow: 200000 },
  { id: 'muse-spark-1.3-contributor-free', contextWindow: 200000 },
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

export function listModelsPayload () {
  return {
    object: 'list',
    data: MODELS.map((m) => ({
      id: `${PROVIDER}/${m.id}`,
      object: 'model',
      created: 0,
      owned_by: PROVIDER,
      context_window: m.contextWindow,
    })),
  }
}