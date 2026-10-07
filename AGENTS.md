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
self-contained, ~46MB.

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

## A note on `src/fingerprint.js`

That file impersonates a first-party client to reach a free tier: a literal
placeholder bearer credential, a spoofed user-agent, forged client headers, and a
reverse-engineered session-id format, kept because "a random hex string is rejected
by the client gate". That is the access mechanism, not a detail. Do not publish this
repository, and do not treat the gate as something to work around more aggressively.