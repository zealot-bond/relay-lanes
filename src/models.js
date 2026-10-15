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
//   GONE chat 410 exo-free                      2.2s on 10-07, 410 on 10-15
//   PASS chat     space-bunny-free              0.8s
//   PASS chat     longcat-2.5-preview-free     4.6s
//   GONE chat 410 fledge-alpha-free             1.9s on 10-07, 410 on 10-15
//
// The gateway's own model list is the one probe available from a machine whose egress
// the free tier refuses, and on 2026-10-15 it proved decisive: it had shrunk from 88
// entries (14 free) to 86 (10 free), and every free model it dropped was one already
// known to fail. nemotron-3-ultra-free was among them -- a THIRD dead model that the
// 410 sweep could not see, because a 403 FreeTierError on every request hides whatever
// the model would have answered.
//
// Seven dropped, all previously broken:
//   deepseek-v4-flash-free   400 model unavailable      ling-3.1-flash-free   endpoint down
//   exo-free                 410 ModelDeprecated       mimo-v2.5-free        410 deprecated
//   fledge-alpha-free        410 ModelDeprecated       nemotron-3-ultra-free (de-listed)
//   ling-3.0-flash-fin-free  400 endpoint unavailable
//
// Being LISTED is not proof of life -- mimo-v2.5-free was listed for days while
// answering 410 on every call -- so this narrows the catalogue, it does not verify it.
//
// Free-suffixed and newly listed on 2026-10-15, NOT added: none could be probed from a
// gated egress, and adding an unprobed model is how ling-3.1-flash-free got in.
//   cat-jam-free   deepseek-v4.1-flash-free   step-5-preview-free
//
//   GONE chat 410 fledge-alpha-free             1.9s on 10-07, 410 on 10-15
//   PASS chat     ling-3.1-flash-free           1.4s
//   PASS chat     big-pickle                    4.3s
//   PASS chat     nemotron-3-ultra-free         5.1s
//   PASS chat     nemotron-3.5-lightning-free 18.3s
//   PASS chat     mimo-v2.6-flash-free          2.0s
//   PASS responses muse-spark-1.2-contributor-free 4.5s
//   PASS responses muse-spark-1.3-contributor-free 1.8s
//
//   FAIL chat 429 "Endpoint is unavailable"    ling-3.1-flash-free (worked at 1.4s earlier; down now --
//        the gateway reports an OUTAGE with status 429, so it was mistaken for a rate limit)
//   FAIL chat 400 "Endpoint is unavailable"    ling-3.0-flash-fin-free
//   FAIL chat 400 "Model is unavailable"       deepseek-v4-flash-free
//   FAIL chat 500 / responses 500              jev-1.13-free
//   FAIL chat 410 "ModelDeprecated"            mimo-v2.5-free
//   FAIL chat 410 "ModelDeprecated"            exo-free          (2026-10-15, was PASS 2.2s)
//   FAIL chat 410 "ModelDeprecated"            fledge-alpha-free (2026-10-15, was PASS 1.9s)
//
// Re-probed 2026-10-15, every entry in the catalogue below. Two had been REMOVED by
// the provider since the sweep above and now answer 410 ModelDeprecated on every call:
// exo-free and fledge-alpha-free. Both were in the "reliable" list handed to the user
// on 2026-10-07, so they were removed here rather than left to fail.
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
  { id: 'nemotron-3.5-lightning-free', contextWindow: 200000 },
  { id: 'mimo-v2.6-flash-free', contextWindow: 200000 },
  { id: 'muse-spark-1.2-contributor-free', contextWindow: 200000 },
  { id: 'muse-spark-1.3-contributor-free', contextWindow: 200000 },
  // exo-free and fledge-alpha-free were here until 2026-10-15 and are gone: the
  // provider answers 410 ModelDeprecated. Removing them is not cosmetic -- an advertised
  // model that cannot be served costs every client six upstream calls before the
  // quarantine finally refuses it.
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