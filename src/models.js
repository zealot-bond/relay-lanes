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

// KNOWN MODEL BEHAVIOUR, measured 2026-10-15 by calling upstream directly and comparing
// with what the relay delivers (streaming and non-streaming agree exactly):
//
//   muse-spark-1.3-contributor-free  OMITS `description` from its bash arguments -- 2/2
//   muse-spark-1.2-contributor-free  includes it -- 2/2
//   big-pickle                       includes it -- 2/2
//
// The relay is faithful -- it forwards exactly what upstream sends. But a harness whose Bash
// schema marks `description` required will reject every muse-spark-1.3 tool call, and the
// symptom is a client-side "missing required property" that looks exactly like relay
// corruption. If you hit that, use muse-spark-1.2, or drop `description` from `required`.
//
// Both muse models are the RESPONSES dialect, so their arguments are assembled from
// response.function_call_arguments.delta rather than delta.tool_calls[].function.arguments.
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
// Seven dropped from /zen/v1/models:
//   deepseek-v4-flash-free   400 model unavailable      ling-3.1-flash-free   endpoint down
//   exo-free                 410 ModelDeprecated       mimo-v2.5-free        410 deprecated
//   fledge-alpha-free        410 ModelDeprecated       nemotron-3.ultra-free (no direct evidence)
//   ling-3.0-flash-fin-free  400 endpoint unavailable
//
// CORRECTION (2026-10-15, later the same day): it was concluded that every model the list
// dropped was therefore broken, and nemotron-3-ultra-free was removed on that basis. That
// inference is WRONG. /zen/v1/models is a static list, not a health signal, and the
// authoritative one -- the models.dev catalogue that OpenCode v2 actually loads -- still
// carries all 123 models for the `opencode` provider, including nemotron-3-ultra-free,
// exo-free, mimo-v2.5-free and deepseek-v4-flash-free, every one of which is confirmed
// dead. Neither list predicts anything. Only a live probe does.
//
// The 410 responses for exo-free and fledge-alpha-free ARE real evidence and those two
// stay removed.
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
// Listed on 2026-10-16 but NOT usable -- 23 of the 26 free ids not already here. Being
// listed is not availability, and the failure is usually the provider's:
//
//   HTTP 400 "Upstream request failed: Model is unavailable."
//     glm-4.7-free, glm-5-free, hy3-free, hy3-preview-free, kimi-k2.5-free,
//     laguna-s-2.1-free, ling-2.6-flash-free, ling-3.0-flash-free, ling-3.0-tiny-free,
//     longcat-2.0-free, mimo-v2-flash-free, mimo-v2-omni-free, mimo-v2-pro-free,
//     minimax-m2.1-free, minimax-m2.5-free, minimax-m3-free, nemotron-3-super-free,
//     north-mini-code-free, qwen3.6-plus-free, ring-2.6-1t-free,
//     trinity-large-preview-free, x-preview-f-free
//
//   HTTP 429 "Upstream request failed: Endpoint is unavailable."  (an OUTAGE, not a limit)
//     glm-5.3-flash-free -- was down on 2026-10-16 and answering on 2026-10-17, so it is
//     ADDED, with the intermittency recorded above rather than treated as a deletion.
//
//   HTTP 400, still unavailable 0/3: deepseek-v4-flash-free, ling-3.0-flash-fin-free,
//     ling-3.1-flash-free
//   HTTP 410 ModelDeprecated 0/3:  fledge-alpha-free, mimo-v2.5-free
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
  // nemotron-3-ultra-free was removed on 2026-10-15 because it was absent from
  // /zen/v1/models, then restored the same day: that list is not a health signal, so its
  // absence proved nothing. It is still absent there and still present in the models.dev
  // catalogue, which is ALSO not a health signal (it still lists exo-free and
  // mimo-v2.5-free, both confirmed 410). Absence is evidence of nothing; only a live
  // probe is.
  { id: 'nemotron-3-ultra-free', contextWindow: 200000 },
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
  // Newly listed free models, added 2026-10-15 and PROBED before being kept -- the first
  // time any of these could be verified at all, because direct mode finally reached the
  // upstream from this host. Measured: cat-jam-free 4/4 plain + 3/3 tools (1.9s p50,
  // the fastest here), deepseek-v4.1-flash-free 4/4 + 3/3 (5.5s), step-5-preview-free
  // 4/4 + 3/3 then 0/5 then 200 again minutes later.
  //
  // step-5-preview-free is genuinely flaky: it returned
  //   HTTP 429 {"error":{"type":"server_error","message":"Upstream request failed:
  //                Endpoint is unavailable."}}
  // which classifyUpstream reports as a MODEL fault (outage), not an exhausted bucket --
  // the same outage-reported-as-429 that affected ling-3.1-flash-free. Do not remove it on
  // one bad sweep, and do not trust a single sweep either way.
  { id: 'cat-jam-free', contextWindow: 200000 },
  { id: 'deepseek-v4.1-flash-free', contextWindow: 200000 },
  { id: 'step-5-preview-free', contextWindow: 200000 },
  // Added 2026-10-16 after probing all 42 free ids in the models.dev catalogue. Three of
  // the 26 that were not already here answered 3/3 on a probe AND on a confirmation run:
  //   beam-free, ll-alpha-free, nemotron-3.5-super-vl-free.
  { id: 'beam-free', contextWindow: 200000 },
  // glm-5.3-flash-free was reported unavailable on 2026-10-16 and then answered on
  // 2026-10-17, minutes apart, on all three bases (/inference/v1, /inference/openai/v1 and
  // the legacy /zen/v1) and on the chat dialect. So its endpoint is INTERMITTENT, not gone --
  // the same pattern as ling-3.1-flash-free and nemotron-3.5-super-vl-free. Its whole family
  // (glm-5-free, glm-4.7-free) does answer 400 "Model is unavailable" consistently, so the
  // difference is this endpoint, not the family.
  //
  // Note what it answers when it is down: HTTP 429 "Endpoint is unavailable". That is an
  // OUTAGE reported with a rate-limit status code, and classifyUpstream already treats it as
  // a model fault rather than an exhausted bucket.
  { id: 'glm-5.3-flash-free', contextWindow: 200000 },
  { id: 'll-alpha-free', contextWindow: 200000 },
  { id: 'nemotron-3.5-super-vl-free', contextWindow: 200000 },
  // exo-free is BACK. Removed on 2026-10-07 for 410 ModelDeprecated; answered 3/3 on
  // 2026-10-16. A withdrawal is not necessarily permanent, and removing on a single
  // observation discards a working model. fledge-alpha-free was still 410 the same day, so
  // it stays out -- the difference is repeated observation, not the error text.
  { id: 'exo-free', contextWindow: 200000 },
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