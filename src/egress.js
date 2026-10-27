// Bun egress proxy: the upstream call has to leave through Bun's TLS stack.
//
// Why this exists (measured 2026-10-26):
//
// The free tier now fingerprints the TRANSPORT, not just the request. The same
// headers, token, body and session id that answer 200 when sent by Bun answer
// 429 FreeUsageLimitError when sent by Node's https or by curl. The three
// ClientHellos to opencode.ai were:
//
//   opencode v2.1.2 (Bun 1.4.2)  JA3 19d4af7f3a46e687da19563bc297b55a   200
//   Bun 1.4.2 fetch             JA3 19d4af7f3a46e687da19563bc297b55a   200
//   Node 22 https               JA3 978f4a0de175a6b5ba61b6e468768fbc   429
//   curl 8.x                    JA3 6485bc9ddc87435b7dfc8e7148e76c4f   429
//
// and the 429 is dressed up as quota exhaustion (Retry-After until midnight UTC),
// which is why it was misread as an exhausted bucket for so long. The relay runs
// on Node, so every lane looked like a spent IP while a real client on the same IP
// kept working.
//
// So the lane keeps doing everything in Node -- parsing, tool merge, streaming --
// and only the socket to opencode.ai is opened by Bun. Node talks plain HTTP to
// this loopback server; this server forwards with Bun's fetch and streams the
// response back verbatim.
//
// Request shape is still the caller's job: the same gate also requires the
// <env>...</env> block in the system prompt and the four tool names, which
// lane.js guarantees.

const ORIGIN = (process.env.EGRESS_ORIGIN || 'https://opencode.ai').replace(/\/$/, '')
const PORT = Number(process.env.EGRESS_PORT || 0)
const HOP = new Set([
  'host', 'connection', 'keep-alive', 'proxy-connection', 'transfer-encoding',
  'upgrade', 'content-length', 'accept-encoding',
])

const server = Bun.serve({
  hostname: '127.0.0.1',
  port: PORT,
  // A prefill on a very large prompt can be silent for minutes; the lane owns the
  // real timeout (FIRST_BYTE_TIMEOUT_MS). 0 disables Bun's own idle cap so it
  // cannot close a healthy socket first.
  idleTimeout: 0,
  async fetch (req) {
    const url = new URL(req.url)
    if (url.pathname === '/__egress_health') return new Response('ok')

    const headers = new Headers()
    for (const [k, v] of req.headers) {
      if (!HOP.has(k.toLowerCase())) headers.set(k, v)
    }
    const target = ORIGIN + url.pathname + url.search
    const hasBody = req.method !== 'GET' && req.method !== 'HEAD'

    let upstream
    try {
      upstream = await fetch(target, {
        method: req.method,
        headers,
        body: hasBody ? req.body : undefined,
        redirect: 'manual',
      })
    } catch (e) {
      // The lane classifies this as a transport fault and retries elsewhere.
      return new Response(JSON.stringify({
        error: { type: 'egress_error', message: `bun egress: ${e?.message || e}` },
      }), { status: 502, headers: { 'content-type': 'application/json' } })
    }

    const out = new Headers()
    for (const [k, v] of upstream.headers) {
      const lower = k.toLowerCase()
      // Bun has already decoded the body, so the upstream's encoding headers are
      // lies now. Node would try to gunzip plain bytes and fail.
      if (lower === 'content-encoding' || lower === 'content-length' || lower === 'transfer-encoding') continue
      out.set(k, v)
    }
    return new Response(upstream.body, { status: upstream.status, headers: out })
  },
  error (e) {
    return new Response(JSON.stringify({
      error: { type: 'egress_error', message: `bun egress: ${e?.message || e}` },
    }), { status: 502, headers: { 'content-type': 'application/json' } })
  },
})

console.log(`egress ready on 127.0.0.1:${server.port} -> ${ORIGIN}`)
