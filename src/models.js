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
// REMOVAL LEDGER -- 2026-10-17, re-verified with repeated runs rather than one probe.
// A model that fails part of the time is worse than a missing one: the client gets a 502 or
// 503 instead of a clean "not offered", and the failure looks like the relay's fault.
//
//   muse-spark-1.3-contributor-free  0/5  HTTP 410 ModelDeprecated
//       The provider really did withdraw it. Worth stating plainly because `gone` is the
//       most consequential verdict in the relay -- not retried, quarantined for an hour,
//       reported as a permanent withdrawal -- so a false positive there would take a working
//       model out of service for everyone for the hour. The raw body is unambiguous:
//         {"error":{"type":"ModelDeprecated","message":"Model muse-spark-1.3-contributor-free
//          has been deprecated."}}
//       The CONVERTER is not the problem: muse-spark-1.2 uses the identical responses-dialect
//       path, identical tool conversion and identical SSE folding, and answered 5/5 in the
//       same sweep.
//
//   ll-alpha-free                     0/5  HTTP 410 ModelDeprecated
//       Answered 3/3 about six hours earlier. So a withdrawal here is NOT permanent either --
//       which is exactly why exo-free was RESTORED below rather than removed, and why a
//       single observation must never be the basis for a deletion.
//
//   glm-5.3-flash-free                2/5  three answered, then HTTP 429 "Endpoint is
//       unavailable" three times; and 0/5 both before and after that window. A coin flip on
//       every request. It was added on the strength of a 3/3 window and has been cut on the
//       strength of 2/5 and two 0/5s. Its family (glm-5-free, glm-4.7-free) answers
//       400 "Model is unavailable" consistently.
//
//   nemotron-3-ultra-free             1/5  four empty 200s. Recoverable upstream flakiness
//       at its worst; the empty-answer counter has to reach its cap before anything is said.
//
//   step-5-preview-free               0/5  HTTP 400 "Model is unavailable"
//   deepseek-v4-flash-free            0/3  HTTP 400 Model is unavailable (re-checked)
//   ling-3.0-flash-fin-free           0/3  HTTP 400 (re-checked)
//   ling-3.1-flash-free               0/3  HTTP 400 (re-checked)
//   mimo-v2.5-free                    0/3  HTTP 410 (re-checked)
//   fledge-alpha-free                 0/3  HTTP 410 (re-checked)
//
//   Also listed but never available: beam-free's neighbours -- glm-4.7-free, glm-5-free,
//   hy3-free, hy3-preview-free, kimi-k2.5-free, laguna-s-2.1-free, ling-2.6-flash-free,
//   ling-3.0-flash-free, ling-3.0-tiny-free, longcat-2.0-free, mimo-v2-flash-free,
//   mimo-v2-omni-free, mimo-v2-pro-free, minimax-m2.1/m2.5/m3-free, nemotron-3-super-free,
//   north-mini-code-free, qwen3.6-plus-free, ring-2.6-1t-free, trinity-large-preview-free,
//   x-preview-f-free -- all HTTP 400 "Model is unavailable".
//
// Every addition was probed on BOTH dialects. The five new models answer 500 or
// 401 on /v1/responses, so they are chat-only; mislabelling them would surface as
// `input[N] did not match any supported type` at request time rather than as a
// clear failure here.
//
// FULL SWEEP -- 2026-10-20. All 43 free ids in the provider's own catalogue, probed
// directly through the lane's request construction (no relay in the path), so this is
// what each model actually does today. 12 work; 31 do not.
//
//   WORK (12): cat-jam, beam, exo, deepseek-v4.1-flash, minimax-m3.1-flash,
//              nemotron-3-ultra, nemotron-3.5-super-vl, mimo-v2.6-flash, big-pickle,
//              glm-5.3-flash, nemotron-3.5-lightning, longcat-2.5-preview
//
//   HTTP 410 ModelDeprecated -- withdrawn upstream:
//     fledge-alpha-free, ll-alpha-free, mimo-v2.5-free ("Use mimo-v2.6-flash-free
//     instead"), muse-spark-1.2-contributor-free, muse-spark-1.3-contributor-free,
//     space-bunny-free ("Use minimax-m3.1-flash-free instead")
//     Both muse-spark models are now gone, so the responses dialect has no live model.
//     RESPONSES_MODELS stays: the converter is still exercised by the tests.
//
//   HTTP 400 "Model is unavailable" -- unchanged from the 2026-10-17 sweep:
//     glm-4.7-free, glm-5-free, hy3-free, hy3-preview-free, kimi-k2.5-free,
//     laguna-s-2.1-free, ling-2.6-flash-free, ling-3.0-flash-fin-free,
//     ling-3.0-flash-free, ling-3.0-tiny-free, ling-3.1-flash-free, longcat-2.0-free,
//     mimo-v2-flash-free, mimo-v2-omni-free, mimo-v2-pro-free, minimax-m2.1-free,
//     minimax-m2.5-free, minimax-m3-free, nemotron-3-super-free, north-mini-code-free,
//     qwen3.6-plus-free, ring-2.6-1t-free, step-5-preview-free,
//     trinity-large-preview-free, x-preview-f-free, deepseek-v4-flash-free
//
// Three were ADDED on this sweep, and each was then run 5x plain + 5x with tools:
//
//   minimax-m3.1-flash-free  5/5   5/5   2.4s   NEW -- space-bunny-free's replacement
//   nemotron-3-ultra-free    5/5   5/5   2.5s   RESTORED -- 1/5 empty answers on 10-17
//   glm-5.3-flash-free       5/5   5/5   8.5s   RESTORED -- cut on 10-17 at 2/5
//
// glm is why this ledger keeps saying "re-probe": the evidence for cutting it was real
// (three HTTP 429 "Endpoint is unavailable" in one window, then 0/5 either side of it) and
// it now answers 10/10. A model cut for an OUTAGE is not a model that is gone.
//
// TOOL ARGUMENT SHAPE -- measured the same day, and this one bites a harness:
//   big-pickle returned bash {"command":"ls -la /tmp"} with NO `description` on one probe
//   and included it on the next. It is INCONSISTENT, not reliably broken (muse-spark-1.3
//   was reliably missing it, which is what made that one diagnosable). A harness that
//   marks `description` required will reject whichever calls omit it.
//   glm-5.3-flash, minimax-m3.1-flash and nemotron-3-ultra included it 5/5 in the same
//   sweep, so they are the safer choice for a strict harness.
//
// contextWindow is unverified for every entry -- the gateway's listing carries no
// metadata, and big-pickle has been observed serving 852k tokens despite the
// 200000 declared here. Treat it as a floor, not a limit.
export const MODELS = [
  // Ordered by MEASURED latency, fastest first, on 2026-10-20 (median of 5 runs of a
  // ~60-word prompt, warm connection, from the same egress). A client that takes
  // data[0] as its default should get a fast, reliable model and not a 16-second one.
  //
  //   model                             plain    tools    median
  //   cat-jam-free                      5/5      5/5      2.3s
  //   beam-free                         5/5      5/5      2.3s
  //   deepseek-v4.1-flash-free          3/3      3/3      2.4s
  //   minimax-m3.1-flash-free           5/5      5/5      2.4s
  //   nemotron-3-ultra-free             5/5      5/5      2.5s
  //   nemotron-3.5-super-vl-free        3/3      1/1      3.6s   (one 500 before that)
  //   mimo-v2.6-flash-free              3/3      3/3      5.5s
  //   big-pickle                        3/3      3/3      7.7s
  //   glm-5.3-flash-free                5/5      5/5      8.5s
  //   nemotron-3.5-lightning-free       3/3      3/3      9-23s
  //   longcat-2.5-preview-free          3/3      3/3      15-16s
  //
  // The two at the bottom WORK -- they are not broken, they are slow, and the spread is
  // theirs, not the relay's. Latency through the lane pool measured 0.3-0.7s above a
  // direct call for the same model, so a 16-second answer is the model thinking, not the
  // architecture. They stay because a working model is not deleted for being slow; pick
  // one of the first six unless you need what they do.
  //
  // exo-free was REMOVED on 2026-10-22. It had answered 3/3 on 10-20, then the provider
  // withdrew it: three consecutive attempts through the public relay returned 410/404, and
  // the relay quarantined it. A withdrawn model is cut; an outage is not (see glm-5.3 above).
  { id: 'cat-jam-free', contextWindow: 200000 },
  { id: 'beam-free', contextWindow: 200000 },
  { id: 'deepseek-v4.1-flash-free', contextWindow: 200000 },
  { id: 'minimax-m3.1-flash-free', contextWindow: 200000 },
  { id: 'nemotron-3-ultra-free', contextWindow: 200000 },
  { id: 'nemotron-3.5-super-vl-free', contextWindow: 200000 },
  { id: 'mimo-v2.6-flash-free', contextWindow: 200000 },
  { id: 'big-pickle', contextWindow: 200000 },
  { id: 'glm-5.3-flash-free', contextWindow: 200000 },
  { id: 'nemotron-3.5-lightning-free', contextWindow: 200000 },
  { id: 'longcat-2.5-preview-free', contextWindow: 200000 },
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