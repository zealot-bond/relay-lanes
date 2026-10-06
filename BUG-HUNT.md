# Bug-hunt prompt

Paste this into a capable model with the repository attached. It is written to be
self-contained: the reviewer starts from the code, not from this file's claims.

---

You are reviewing a Node.js OpenAI-compatible reverse proxy that obtains its
capacity from GitHub Actions runners, because the upstream free tier rate-limits
**per egress IP**. `java -jar server.jar` unpacks a bundled Node runtime and starts
an HTTP server plus a lane orchestrator. One GitHub Actions job == one runner == one
egress IP == one independent rate-limit bucket.

Read `AGENTS.md` first, then review for defects. `AGENTS.md` describes intended
behaviour; **treat it as a claim to verify, not as ground truth.** If the code
contradicts it, that contradiction is a finding.

## Files, in dependency order

- `src/config.js` — config resolution: env → `start.properties` → `baked-credentials.json`
- `src/queue.js` — `WorkQueue` (FIFO, claim leases) and `LaneRegistry`
- `src/models.js` — catalogue, protocol selection, public id naming
- `src/fingerprint.js` — upstream headers, session-id format, failure classification
- `src/lane.js` — upstream call, two protocol dialects, SSE folding, tool extraction
- `src/worker.js` — the loop that runs inside a runner
- `src/server.js` — client API, dispatch/retry, quarantine, lane endpoints
- `src/orchestrator.js` — keeps the runner pool full
- `src/main.js` — entry point

## Invariants that must hold

1. A client never receives HTTP 429.
2. A request is never silently dropped, and never left in the queue after its
   dispatcher ends.
3. Every dispatch exits through exactly one `queue.finish()`.
4. A retired lane (exhausted bucket) must never work again.
5. Model quarantine expires — a transient upstream failure cannot disable a model
   until restart.
6. Tool calls reach the client intact: one entry per `index`, real indices for
   parallel calls, `id`/`name` present, `finish_reason: "tool_calls"` when calls exist.
7. Client tool specs reach upstream **unioned with** the fingerprint tools.
   Substituting them causes upstream 403.
8. A large-input prefill is never killed by the idle timeout.
9. Text forwarded to a client is never duplicated or interleaved.
10. No client-visible field names the upstream vendor.

## Priority 1 — the bug being seen right now

**Symptom:** latency grows sharply once tools are in play, and the `edit` tool fails
intermittently with:

```
invalid arguments: missing required property "file_path";
missing required property "old_string"; missing required property "new_string"
```

A partial cause was already fixed: streamed tool-call fragments were emitted as
separate `delta.tool_calls` array elements, all sharing `index: 0`, each with
`function.name === null` and no `id`. Batching now merges per index. **Verify the fix
is complete and correct**, and find whatever else contributes — in particular why
tool use specifically inflates latency. Consider at minimum:

- `DELTA_BATCH_MS` batching: one `POST /lane/delta` per 40ms window, per lane. Under
  many tool calls, does the relay become the bottleneck? Is the flush awaited in a
  way that serialises against the upstream read?
- Is `MAX_SOCKETS` / `maxFreeSockets` on the keep-alive agent, or HTTP connection
  limits on the relay, causing queuing when several lanes stream simultaneously?
- Does a tool-heavy turn cause the lane to hold its claim longer, so the pool shrinks
  and later requests queue? Check `claimLeaseMs` against worst-case tool turns.
- Are tool arguments double-counted anywhere — once via `onToolDelta` and again by
  the final fold in `parseUpstream`?

## Priority 2 — correctness sweep

Check specifically:

- **Dialect conversion** (`toResponsesInput`, `toResponsesTools`, `responsesContent`):
  every message role, `role: 'tool'`, assistant `tool_calls`, content as a string vs
  an array, image parts, empty turns, system folding, `output_index` handling.
- **Queue lifecycle**: entry added to `pending`, claimed, requeued, abandoned,
  finished. Can an entry be in two places at once, or in neither? Can `requeue`
  resurrect finished work? Is the lease reaper correct for an entry whose client
  disconnected mid-stream?
- **Dispatch**: `RETRYABLE`, `HEALTH_THRESHOLD`, `entry.committed` interaction with
  streaming, `MAX_HOLD_MS` vs a committed stream, `ATTEMPT_HARD_MS` vs the lane's own
  phase-split timeouts.
- **Streaming**: partial `data:` frames split across TCP reads; SSE frames split
  across chunk boundaries; backpressure; client disconnect mid-stream; double
  `finish_reason`; `[DONE]` always emitted.
- **Lane selection**: can `pickLane` return a lane that is busy, exhausted, or stale?
  Is the sort deterministic under ties?
- **Orchestrator**: the `max(live lanes, github runs)` rule under both known failure
  modes; the hourly budget; cooldown interactions; what happens when the GitHub API
  returns partial data.
- **Config**: can `config.js` read a value at import time before it is resolved? Any
  secret logged?

## How to report

For each finding give: file and line, the concrete input or sequence that triggers
it, the observed behaviour, the expected behaviour, and severity. Rank by severity.

**Separate confirmed defects from speculation and label them as such.** State what
you ran and what the output was. If a claim in `AGENTS.md` is wrong, say so and show
why. Do not propose changes to the model catalogue or to tool filtering without
stating the reason — those were explicit requirements.