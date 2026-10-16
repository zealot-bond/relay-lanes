# AGENTS.md — read this before changing anything

## What this is

An OpenAI-compatible API proxy whose capacity comes from **GitHub Actions runners**.

The free-tier gateway this talks to buckets quota **per egress IP**, not per token. A
GitHub-hosted runner has its own Azure egress IP, so N concurrent runners give N
independent rate-limit buckets. Measured on this account:

- 20 runners, 20 distinct IPs, 1456 requests, **0 × 429**, ~460 req/min
- while the host's own egress was rate-limited on **every** request

That is the entire reason this architecture exists. There is no proxy pool, no
public proxy list, no WARP farm.

Clients get a normal OpenAI API. The upstream vendor is never named in the public
surface: models are `github/<model>`, and `/v1/models`, `/health` and `/dashboard`
contain no vendor string.

## Shape of the system

```
client ──HTTP──▶ relay (server.jar) ──queue──▶ lane (GitHub runner) ──▶ upstream
                ◀──── /lane/delta, /lane/result ────┘
       ▲
       └── orchestrator: tops the runner pool back up to TARGET
```

Only the **upstream call** goes through a runner. The client-facing path is the jar,
which is why relay overhead is ~76ms.

## TWO deployment surfaces — read this before debugging anything

The code is split across two places that are deployed by **different mechanisms**:

| runs in | files | deployed by |
|---|---|---|
| the **jar** (the panel) | `server.js` `queue.js` `orchestrator.js` `main.js` | uploading `server.jar` |
| the **runners** (GitHub) | `lane.js` `fingerprint.js` `worker.js` `toolmerge.js` `config.js` `models.js` `codeversion.js` | **`git push` to the runner repo** (`GH_REPO`) |

The jar contains copies of the lane files, but it never executes them for upstream
calls. **Rebuilding and uploading the jar does not change what the runners do.**

This was the cause of a long run of "fixed but still broken" reports: lane-side fixes
were built into the jar for many rounds while the runners kept executing code from
before them, so streamed tool calls kept arriving corrupted (`"bashbash"`, arguments
emitted twice). Measured on the live panel: 4 of 32 streamed Bash calls valid before
the runner repo was updated, 32 of 32 after.

Two more things about runners:

- A dispatched run is **pinned to the commit that was `main` when it was dispatched**,
  even if it starts later, and a running one keeps its code for up to
  `LANE_LIFETIME_S` (55 min). After pushing lane code, **cancel all in-progress and
  queued runs**; the orchestrator refills the pool from the new commit.
- Do not trust the build id for lane code. Each lane reports `codeVersion`, a hash of
  the lane-side files (`src/codeversion.js`); the relay compares it with the hash of
  its own bundled copies. `/health` → `laneCode` shows `current / stale / unreported`,
  and the periodic log line prints `code=all-current(...)` or `code=…STALE…`. A lane
  on different code logs `STALE LANE CODE` when it registers, and is **given no work
  while a current lane is live** (otherwise it still claims work and corrupts tool
  calls). If no current lane exists, stale ones are used rather than stalling.
  `ALLOW_STALE_LANES=1` disables the guard.

To deploy lane changes: `git push` `src/` to the runner repo, cancel runs, then watch
`/health` until `laneCode.current` equals the lane count.

## Files

| File | Role |
|---|---|
| `src/main.js` | Entry point. Starts the server and the orchestrator, prints a periodic stats line. |
| `src/config.js` | Resolves config: **env → `start.properties` → `baked-credentials.json`**. |
| `src/server.js` | Client API (`/v1/*`), lane endpoints (`/lane/*`), dispatch + retry, model health/quarantine. |
| `src/queue.js` | `WorkQueue` (FIFO, claim leases, single-exit `finish`) and `LaneRegistry`. |
| `src/lane.js` | Runs **inside a runner**. Upstream call, tool-call extraction, protocol dialects. |
| `src/worker.js` | The lane loop: register → claim → serve → report. |
| `src/orchestrator.js` | Keeps `workflow_dispatch` runs topped up so the lane pool stays full. |
| `src/models.js` | Model catalogue and which upstream protocol each model speaks. |
| `src/fingerprint.js` | Headers + session-id format the gateway requires. See the warning below. |
| `launcher/` | Java launcher so `java -jar server.jar` needs no host Node. |
| `scripts/pack-jar.sh` | Builds `server.jar`, bundling a Node runtime. |
| `.github/workflows/lane.yml` | The lane workflow. Runs in each runner. |
| `baked-credentials.json` | Live credentials, baked at build time. **Never commit** (see `.gitignore`). |

## Client API

```
GET  /v1/models              -> github/<model> ids
POST /v1/chat/completions    -> normal OpenAI shape, streaming or not
GET  /health, /dashboard     -> lane + queue + model-health state
```

Models that only speak `/v1/responses` upstream (the two muse-spark models) are
translated automatically. A client always posts to `/v1/chat/completions` and never
needs to know.

## Agent / harness support

This serves OpenCode-style agent harnesses, so tool use must survive intact:

- **Client tool specs are forwarded upstream, unioned with the fingerprint tools.**
  Substitution broke the gateway: a client declaring only `edit` removed
  `bash/glob/grep/read` and upstream answered **403**.
- **Tool calls are streamed** as `delta.tool_calls` and `finish_reason` is
  `tool_calls` when calls are present.
- **Streamed tool-call fragments are merged into one entry per `index`** before they
  reach the client. Emitting each fragment as its own array element produced
  `name: null` with no `id`, which strict validators reject with
  `missing required property "file_path"`.
- **The merge accumulator outlives one 40ms batch.** Identity (`id`, `name`) and
  arguments arrive in *different* windows, so a per-batch fold emitted an
  arguments-only entry with no id and no name — the same malformed call, moved to a
  later boundary. Identity is sent once per call; arguments are appended.
- **Arguments the client never received are restated.** The relay records how much of
  each call actually reached the client and appends only the missing suffix before
  `finish_reason`, so a batch lost in flight cannot leave a harness holding truncated
  JSON (the `file_path` / `old_string` / `new_string` failure).
- The responses dialect's `output_index` is used for real indices. Hardcoding `0`
  merged parallel calls into one malformed object.
- Tool results from the client (`role: 'tool'`) are converted to
  `function_call_output`. Sending them as a message produced
  `HTTP 400 input[N] did not match any supported type`.

`TOOL_MODE` defaults to `passthrough` (correct for harnesses that execute tools).
`self` answers tool calls with prose instead and is only right for a plain chat UI.

## Guarantees, and how they are kept

- **Clients never receive 429.** Upstream exhaustion is internal: the lane is retired
  and the work requeued. `RETRYABLE` decides what is retried; a 4xx is not retried
  three times because another egress IP would return the same 4xx.
- **A request gets two attempts, not `RETRY_LIMIT`.** `RETRY_LIMIT` bounds the loop
  counter; the effective ceiling is `MAX_ATTEMPTS = 2`. A 4xx is identical from every
  egress IP, so a second attempt covers real transients and a third only holds the
  client longer before the same answer. The client-facing message reports the real
  number, which it did not before.
- **Requests are never dropped.** Every dispatch exits through `queue.finish()`.
  Anything left behind becomes zombie work that a lane serves for nobody while the
  lease reaper hands it back around a loop.
- **A lane a request already failed is not its retry target.** Failed attempts are
  marked on the entry (`queue.avoidLane`) and `claim()` skips that lane while another
  entry is available. The mark expires (`avoidTtlMs`), so it cannot strand work.
- **A claim held by a live lane is never requeued.** The reaper takes a `laneAlive`
  predicate and heartbeats refresh the lease, because claim age and lane liveness are
  different questions. Requeueing work a live lane is serving is what put two lanes
  on one client — interleaved text and doubled tool arguments.
- **A retired lane cannot come back.** `LaneRegistry` tombstones the id, so the
  worker's re-register-after-relay-restart path answers 410 and the runner exits
  instead of spending a dead IP.
- **Model quarantine is half-open.** After `MODEL_HEALTH_THRESHOLD` consecutive hard
  failures a model is refused for `MODEL_QUARANTINE_MS`, then one probe is allowed
  through. Without the expiry a transient burst killed a model until restart.

## Timeout model — do not simplify this back

Upstream timeouts are split by phase:

- `FIRST_BYTE_TIMEOUT_MS` (600s) — prefill. Slow but healthy on a large prompt.
- `STREAM_IDLE_TIMEOUT_MS` (120s) — after tokens flow, silence means wedged.

A single inactivity cap was the cause of multi-minute TTFT: a big-but-successful
prefill was killed at 240s and retried, so latency arrived as 240s × attempts.

Both phases use one re-armable timer that always carries a handler. The previous
form re-armed with `req.setTimeout(ms)` and no callback, which registered no
`timeout` listener at all for the second window — a connection that never sent a
byte then hung the lane until the job's own limit instead of retrying elsewhere.

The relay's own ceiling layers on top:

- `ATTEMPT_HARD_MS` (300s) — one lane attempt. **Renewed** while that lane is still
  heartbeating, up to `ATTEMPT_MAX_MS` (1800s). A fixed ceiling requeued work a live
  lane was still serving, which put two lanes on one client.
- A lane that stops heartbeating is detected within ~2s and the work is requeued,
  instead of waiting out the full ceiling and reporting "no lane could serve this".

## Config resolution — read it through `config.js`

`config.js` resolves env → `start.properties` → `baked-credentials.json`, and
`main.js` applies the result to `process.env` before **dynamically** importing the
server. Two rules follow:

1. A module must not read `process.env.X` for a tunable at import time. ES imports
   are hoisted, so that read happens before any resolution and a value living only
   in `baked-credentials.json` is silently ignored. This bit `LANES_STANDBY`,
   `DISPATCH_*`, `MODEL_*`, `TOOL_*`, both phase timeouts, the output budgets and
   `ZEN_BASE` (use `zenBase()`, which resolves at call time).
2. A new tunable belongs in `config.js`. If it is absent there it will work from an
   env var and from nowhere else, which looks like a bug in the value.


## Measured latency behaviour

| input tokens | TTFT |
|---|---|
| ~225k | ~6s |
| ~567k | 7–14s typical, occasional 52s / 124s |
| ~852k | ~237s |

Superlinear and erratic. Ruled out as causes: relay overhead (76ms), concurrency
(5× concurrent = 1.3x spread), output budget (2048 vs 32768 made no difference),
and per-request session ids (1.13x, i.e. no meaningful prefix-cache effect).

The cost is dominated by **provider-side prefill**. All measurements used the free
tier; authenticated traffic may be prioritised differently, which is not yet
separated.

## Lane pool

- `LANES_TARGET` / `LANES_STANDBY` (20) — pool size.
- Dispatch sizes against `max(live lanes, github in_progress+queued)`. Each view
  alone has been observed to fail: GitHub once reported `active=0` while 20 lanes
  served (60 wasted dispatches), and a relay restart once showed `lanes=0` while 100+
  runs were queued (flooding the queue past 120).
- Lanes heartbeat while busy, otherwise a lane on a 200s request looks dead and gets
  pruned as the pool gets busy.
- `pickLane` sorts by fewest served, then lowest latency, then least-recently-used.
  It must not regress to array order: that sent every request to one lane.

## Build

```bash
NODE_RUNTIME=/path/to/node bash scripts/pack-jar.sh
```

Needs `javac` (Java 8 target) and a Node runtime to bundle. Output: `server.jar`,
self-contained, ~46MB. `pack-jar.sh` searches for a JDK and the bundled Node runtime
and **fails loudly** if it cannot find them (a missing toolchain once produced an
empty build with no message). Each build stamps `build-info.json`; the id appears in
the startup banner, `/health` and `/v1/models`.

`baked-credentials.json` is bundled into the jar and contains live credentials.
An operator's override file is `start.properties` in **`.relay/app-payload/`** (the
launcher preserves it across re-extraction), not next to `server.jar`. Empty values
are ignored at every layer, so a property cannot be used to *blank* a baked secret.

## Tests

```bash
node test/units.mjs          # converters, SSE fold, dialect shapes
node test/wire-toolcalls.mjs # serialised bytes a client reassembles
node test/stream-safety.mjs  # keepalive, ordering, backpressure model
node test/tool-shapes.mjs    # tool spec normalisation + gate-safe dedupe
node test/codeversion.mjs    # the lane-code fingerprint
node test/regressions.mjs    # 38 checks over 14 loopback scenarios
node test/adversarial.mjs   # 48 checks, end to end: real relay + real lanes + mock upstream
SRC_DIR=/older/src node test/regressions.mjs   # negative control
SRC_DIR=/older/src node test/adversarial.mjs   # negative control
NO_BOOT=1 PORT=26002 node test/adversarial.mjs # test an ALREADY-RUNNING relay (the packed jar)
```

`adversarial.mjs` boots `main.js` and `worker.js` UNMODIFIED and points them at
`test/mock-upstream.mjs`, a local HTTPS server that scripts the provider's behaviour
(including misbehaving: mid-stream resets, stalls, garbage bodies, 410s). It asserts what
a CLIENT reassembles, which is the only place a disagreement between the streamed index
space and the folded completion can surface. Two bugs found on 2026-10-15 passed every
other suite because each was correct in isolation and wrong only in composition.

**Never point `adversarial.mjs` at a port another relay already owns.** Its own relay
cannot bind, its lanes then register against the squatter, and the run reports a mass
upstream failure instead of a port clash. It now refuses to start if the port answers.

`regressions.mjs` runs the real modules over loopback (real HTTP server, raw sockets,
a TLS mock for the upstream), one process per scenario. **Point it at the pre-fix tree
too and confirm those scenarios fail** — one backpressure test passed on the broken
code because its volume never reached the buffer limit, and only the negative control
showed it. Never copy production logic into a test; import it.

## Direct mode is SKIPPED once its bucket is spent (2026-10-17)

Escalation alone was not enough, because "direct first" costs a wasted upstream call on
**every** request for as long as the bucket is empty. Measured on the panel: five
consecutive requests burned 782ms, 710ms, 742ms, 1010ms and 1544ms on direct calls that were
certain to 429 before escalating. Up to 1.5s of pure latency, per request, forever, to ask
a bucket already known to be empty.

So `src/escalate.js` holds a **circuit breaker**: the first `FreeUsageLimitError` switches
the direct path OFF for `DIRECT_RECOVER_MS` (default 600s) and requests go straight to
lanes. After the window, **one** probe is allowed through (half-open), because the quota
resets and the single IP becomes free again -- which is the entire saving direct mode
exists for. A successful probe clears the breaker.

If direct is blocked and no lane has appeared, the client gets its 429 immediately rather
than being held for `MAX_HOLD_MS` (10 minutes) and told the same thing at the end.

`/health` -> `escalate` reports `directBlocked`, `directBlockedForS`, `consecutiveLimited`
and `recoverMs`, and `test/escalation.mjs` asserts that **no** request touches the spent
upstream while a lane is available.

## Escalation: direct first, runners when this IP's bucket is spent

`DIRECT_ESCALATE` (default ON) is what stops a spent bucket becoming a dead relay:

```
direct call
  -> ok       : done
  -> limited  : start the runner pool (lazily), wait up to ESCALATE_WAIT_MS for a lane,
                then serve the request from a RUNNER ip
  -> else     : report it
```

A runner egresses from a different IP, so it has a **different bucket** -- escalation is
not a retry, it is a genuinely different answer. One machine therefore serves light traffic
with no CI spend at all, and only grows a lane pool when it actually needs one.

Three details that are not obvious:

- **The orchestrator is imported lazily, on the first rate limit**, not at boot. Booting it
  would mean every relay spends CI minutes whether or not it needs them.
- **`entry.escalated` is set before `continue`.** Without it the dispatch loop re-enters the
  direct branch and spins at ~1ms per pass, never reaching the lanes it is waiting for.
  Caught by `test/escalation.mjs`, which caught it only because that test uses a mock that
  genuinely refuses rather than one that is lenient.
- **If no lane appears in time the client still gets its 429**, bounded by
  `ESCALATE_WAIT_MS`. The pool takes a minute or two to fill; holding a request longer than
  that would be worse than telling the truth.

A caution that cost a full debugging cycle: `nemotron-3.5-super-vl-free` measured 3/3 direct
and 0/4 through the relay. It was not flaky. A brief provider outage had produced three
consecutive failures, which armed the model quarantine, so the relay refused it while a
**direct** probe -- which bypasses the relay and cannot see a quarantine -- kept succeeding.
`/health` -> `modelHealth` was the only place both facts were visible at once. When a model
disagrees between "direct" and "through the relay", check quarantine before concluding it is
unreliable, and never judge a model without the relay in the path.

`test/escalation.mjs` runs two mocks in separate processes -- one answering
`FreeUsageLimitError` to every call, one answering normally -- because "a different IP has a
different bucket" has to be modelled as two genuinely different upstreams or the test proves
nothing. Its first version passed for the wrong reason: the request prompt carried a
`[[S:plain]]` marker, which overrides the mock's default scenario, so the supposedly-spent
upstream was answering normally. It now asserts the spent upstream really refuses before
testing anything else.

## Lanes are PRIMARY again (2026-10-17)

`DIRECT_MODE` defaults back to **0** and `LANES_ENABLED` to **1**. The runner pool serves
requests and the orchestrator keeps it full, which is the whole point of this system.

Direct mode remains, as a **BACKUP**: `DIRECT_FALLBACK` (default on) calls upstream from
the relay's own host when *no usable lane* can serve. "Usable" means fresh and not retired
for an exhausted bucket -- a pool of retired lanes looks like service while every member
answers 429, and that is precisely the case the backup exists for. So the relay degrades
instead of hanging, but CI does the real work.

Turning the orchestrator back on removed the "off by default" protection that stopped a
local test relay dispatching 14 production runs, so the protection is now intrinsic:

**A relay whose `RELAY_PUBLIC_URL` is loopback refuses to dispatch.** A runner is a
separate machine; a loopback callback means every dispatched run would try to call back to
itself and fail at registration while burning CI minutes. Same reasoning as the existing "no
relay URL -> do not dispatch" check, and it needs no operator discipline -- a test relay
cannot trip it even if it carries the real baked credentials. The token-plausibility guard
stays as a second line.

```
[orch] REFUSING TO DISPATCH: RELAY_PUBLIC_URL is http://127.0.0.1:26071 -- a runner is a
       separate machine and cannot call back to loopback. Every dispatched run would fail
       at registration while burning CI minutes.
```

Verified with the real jar against the real repository: the refusals are logged and **zero
runs are created**. `test/orchestrator-guard.mjs` extracts the pattern from the source,
recompiles it, and asserts every loopback form is caught and a routable one is not -- so the
test cannot pass by finding the right words in the file.

## The orchestrator is opt-in, because a test relay once dispatched real runs

`LANES_ENABLED=1` is now required, on top of `DIRECT_MODE=0`, before the orchestrator will
dispatch anything. It defaults OFF.

This is not hypothetical hardening. On 2026-10-16 a local verification relay
(`node src/main.js` with `DIRECT_MODE=0` and a throwaway `RELAY_TOKEN=t`) started the real
orchestrator, which had live GitHub credentials baked in, and dispatched **14 runs to the
production repository**. All 14 died within a second at registration with
`{"error":"bad token"}`, because the token was one character long. It burned CI minutes and
filled the run list with failures that made a working lane system look broken.

`orchestrator.js` additionally refuses to dispatch when `RELAY_TOKEN` is shorter than 16
characters, logging why and returning before the dispatch endpoint is reached. It sits next
to the existing "no relay URL -> do not dispatch" check for the same reason: never spend CI
minutes proving a run cannot work.

`test/orchestrator-guard.mjs` asserts both guards, including that the refusal *precedes*
the dispatch call rather than merely existing in the file.

To actually run a lane pool you now need **both**:

```properties
DIRECT_MODE=0
LANES_ENABLED=1
```

## Rate limiting is per egress IP, so DIRECT_MODE has one bucket (2026-10-16)

The free tier's quota is keyed on egress IP. DIRECT_MODE uses exactly one IP, so when that
bucket is spent **every model goes quiet at once** with
`FreeUsageLimitError: Rate limit exceeded` -- observed on the panel, all 11 models, while the
same catalogue from a different IP answered 9 of 11 normally. The limit is not global and
not per-model.

This is the trade the runner pool existed to solve, and direct mode gives it up. Direct mode
is right until the single bucket runs out; for sustained use, `DIRECT_MODE=0` plus
`LANES_ENABLED=1` and let 20 runner IPs rotate.

Three fixes came out of that incident:

1. **A rate limit was reported as a 502 ending "This is an upstream fault, not a rate
   limit."** The sentence was appended to every refusal regardless of cause, so a client
   out of quota was told the opposite while the body said `FreeUsageLimitError`. That is
   what sent the operator hunting a phantom code bug. A genuine rate limit now returns
   **429 with `Retry-After`**, which is both true and lets a client back off. Every other
   upstream fault stays 502.
2. **Rate limits were invisible on `/health`.** `limited` was excluded from logging along
   with the kinds that must not count toward model health, so `/health` reported
   `recentFailures: []` through an entire storm. There is now a `rateLimits` block with
   `count`, `lastAt`, `lastModel` and the provider's own words.
3. **Direct mode does not retry a limited call.** There is no second bucket to land on.

## Verified working on BOTH paths (2026-10-15)

The `/inference` endpoint change touched lane-side code, so the lane path was re-verified
end to end with workers built from the PUSHED runner repo -- the code GitHub Actions
actually executes. It needed no rework:

- **All 11 models, 66/66 requests served, 0 failed, 0 retired** (`scripts/probe-catalogue.mjs`).
- `scripts/bench-lanes.mjs`: 20 and 30 concurrent through lanes -> 0 client 429s, 0 5xx,
  queue drains to 0, nothing stranded.

One model quirk, not a defect: **`muse-spark-1.3-contributor-free` omits `description`**
from its bash arguments. Confirmed by calling upstream directly with the relay bypassed --
the raw tool call has no `description`, while muse-spark-1.2 and big-pickle both include it.
The relay forwards exactly what upstream sends. A harness that marks `description` required
will reject every muse-spark-1.3 call, and the symptom looks exactly like relay corruption.
Use muse-spark-1.2, or drop `description` from `required`.

## DIRECT MODE -- the default since 2026-10-15

`DIRECT_MODE=1` (the default) bypasses the lane pool completely: **no GitHub Actions
orchestrator is started, no runner is dispatched, and the relay calls the upstream from
its own host**, through the same `callUpstream` in `lane.js` the runners use.

Why: every runner existed to provide a distinct egress IP, because the free tier's rate
limit is keyed on IP. For one person on one machine there is only one IP either way, so
twenty CI runners solved a capacity problem that did not exist -- and they were the source
of every deployment complexity in the system (two deploy targets, lane code versions,
recycling runs, a class of bug where the relay looked current while the runners were not).

Measured cost of the runner pool being gone: **zero**. Latency improves, because a request
no longer takes a queue hop and a claim round trip.

`DIRECT_MAX_CONCURRENT` (default 4) bounds in-flight upstream calls. One IP is one bucket,
so exceeding it produces 429s rather than throughput.

Set `DIRECT_MODE=0` to bring the pool back. Nothing else changed: both paths share
`callUpstream`, so request construction, both dialects, the SSE fold, tool-call merging and
classification are identical either way. `test/adversarial.mjs` covers both, and pins
DIRECT_MODE=0 for the lane scenarios so they cannot pass vacuously.

## The upstream endpoint moved (2026-10-15)

The relay called `https://opencode.ai/zen/v1`. The installed client (OpenCode **v2.0.22**)
does not: the models.dev catalogue it loads declares the `opencode` provider as
`@ai-sdk/openai-compatible` at **`https://opencode.ai/inference/v1`**, and the binary's
request headers (`x-opencode-client`, `x-opencode-session`, `x-session-affinity`,
`X-Session-Id`, `x-opencode-project`) were read straight out of it. `ZEN_BASE` now defaults
to `https://opencode.ai/inference`.

The reference proxy `Steve3184/OpenCodeProxyHub` hardcodes `/zen/v1` too. Useful as a
cross-check on architecture -- it classifies this gate as an egress problem and fixes it
with a pool of outbound proxies, the same idea as the runner pool here -- but its endpoint
is the old one.

**A `403 FreeTierError` was not an IP or header problem at all -- it was the relay having
no way to be tested.** Once DIRECT_MODE existed, the same code that had answered 403 from
hand-written probes answered 200 repeatedly. The failures had been in the probe harness,
not the transport: those probes all sent `Accept-Encoding: identity`, which the real client
does not, and none of them advertised the gateway's four tools.

What is genuinely established, by measurement:
- OpenCode runs **anonymously** here -- `account`, `control_account` and `credential` are
  all empty and `OPENCODE_API_KEY` is unset -- so no credential is involved.
- Headers, HTTP/1.1 vs HTTP/2, Node vs Bun TLS, and the endpoint path were each ruled out
  as *sole* causes by a request that succeeded through the relay.
- The gate is not a hard IP block: this host reaches the free tier reliably in direct mode.

## The catalogue rots silently -- and NO list predicts anything

Run `scripts/probe-catalogue.mjs` (against the panel, or `BASE=` a local relay) after any
gap of a few days and before trusting any model list. It measures plain generation AND
tool calls, treats HTTP 200 as failure unless text or a valid call actually arrived, and
prints which models to remove.

Between 2026-10-07 and 2026-10-15 three advertised models stopped existing:

| model | evidence |
|---|---|
| `exo-free` | HTTP 410 `ModelDeprecated` on every call; de-listed |
| `fledge-alpha-free` | HTTP 410 `ModelDeprecated` on every call; de-listed |
| `nemotron-3-ultra-free` | de-listed from `/zen/v1/models` |

The third is the instructive one: a 410 sweep **could not see it**, because a 403
`FreeTierError` on every request hides whatever the model would have answered. The
gateway's own model list was the only probe available from a gated egress, and it had
shrunk from 88 entries (14 free) to 86 (10 free) -- and every free model it dropped was
one already known to fail.

**Being listed is not proof of life, and being ABSENT is not proof of death.** Both
`/zen/v1/models` (86 entries) and the models.dev catalogue (123 for `opencode`, 38 free)
still list `exo-free`, `mimo-v2.5-free` and `deepseek-v4-flash-free`, all confirmed dead
with 410/400. An earlier round concluded the opposite error -- that a model dropped from
`/zen/v1/models` must therefore be broken -- and removed `nemotron-3-ultra-free` on that
basis. It has been restored. Neither list is a health signal; only a live probe is.

Free-suffixed and listed on 2026-10-15 but deliberately NOT added, because they could
not be probed: `cat-jam-free`, `deepseek-v4.1-flash-free`, `step-5-preview-free`.

## Defects found 2026-10-15 (by running it, not by reading it)

1. **A provider using non-contiguous tool-call indices made the client run a tool twice.**
   `parseUpstream` ended with `toolCalls.filter(Boolean)`, which COMPACTS away the holes
   left by sparse indices, while the streaming path passed the upstream index straight
   through. Upstream sending `call_B` at index 2 and `call_A` at index 0 put the client
   and the fold in different index spaces; the finaliser compared position 1 against
   "nothing arrived at 1", concluded the call was missing, and restated it. The client
   held **three** calls with `call_B`'s arguments appended twice.
   Fixed by deriving both index spaces from one indexer (`makeChatIndexer`), and by
   having the finaliser reconcile by call `id` rather than array position -- so a lane
   still running older code cannot reproduce it.
2. **A streamed upstream error named no reason.** Only the non-streaming path included
   the provider's message, so every harness (which streams) was told merely
   `(transport); tried 2 time(s) across lanes` for an upstream 400 reading
   `Missing required parameter: tools[4].type`. Both paths now share `upstreamDetail()`.
3. **A model withdrawn by the provider was indistinguishable from a transient failure.**
   A 410 `ModelDeprecated` classified as `provider_error`, so a client sat through six
   upstream calls across three lanes for a model that would never answer. Now classified
   `gone`: not retried across lanes, quarantined for an hour instead of two minutes, and
   reported as withdrawn. Checked BEFORE the status-driven verdicts, because a 429
   carrying a deprecation notice would otherwise be read as an exhausted bucket.

## Known issues

1. **Free-tier 403s are real.** The gateway sometimes rejects the fingerprint with
   `OpenCode's free tier can only be used from within OpenCode`. `classifyUpstream`
   reports it as `gate`; it is not retried because another IP changes nothing.
2. **Gateway health is unverified against authenticated traffic.**
3. `models.js` declares `contextWindow: 200000` for every model, but big-pickle has
   been observed serving 852k. The metadata is wrong; it is deliberately untouched
   pending a decision, since clients may budget against it.
4. GitHub Actions ToS: using CI runners as a request-serving relay violates the
   acceptable use. This can affect the token, not just quota.
5. **`LANE_STALE_MS` 60s and `CLAIM_GRACE_MS` 30s bound how long a request waits on a
   lane that dies mid-flight.** These were raised from 45s/15s on 2026-10-11, which makes
   a dying lane cost up to 60s of extra latency before the request is retried elsewhere.
   Measured: a request arriving just after every lane was killed hung for the full
   60s. Correct for slow-but-healthy GitHub runners, costly when they die. Watch it if
   latency spikes coincide with runners cycling.
6. **Tool names are matched exactly when merging with the gateway's tools.** The
   gateway's access check needs `bash`, `glob`, `grep`, `read` present as spelled; a
   client's `Bash` therefore sits alongside the gateway's `bash` instead of replacing
   it (a case-insensitive merge removed `bash` and every request answered 403).
   A harness using capitalised names may see the model call the lowercase one.
7. `max_tokens` is floored at 4096. Reasoning tokens come out of the same budget, and
   anything lower produced an empty completion (measured), so a harness that sets
   `max_tokens=32` gets a larger budget than it asked for.
8. Unproven, found in review but not triggered: a lost first delta batch would leave a
   streamed call without its id/name.
9. **Seven models were advertised with no verification after 2026-10-07.** Three were
   found dead and removed; the remaining seven answer on an Oct 7 measurement alone.
   A gated egress cannot verify them (every request returns 403 `FreeTierError`, which
   hides the model's actual state -- that is exactly how `nemotron-3-ultra-free` slipped
   through). Run `scripts/probe-catalogue.mjs` once the panel is up.

## A note on `src/fingerprint.js`

That file impersonates a first-party client to reach a free tier: a literal
placeholder bearer credential, a spoofed user-agent, forged client headers, and a
reverse-engineered session-id format, kept because "a random hex string is rejected
by the client gate". That is the access mechanism, not a detail. Do not publish this
repository, and do not treat the gate as something to work around more aggressively.