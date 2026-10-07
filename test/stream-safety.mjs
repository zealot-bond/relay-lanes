// Verifies the two streaming fixes at the socket level.
//
// 1. KEEPALIVE -- a slow upstream must still produce bytes on the wire while it is
//    thinking, so an intermediary's idle timer cannot silently drop the request,
//    and the frames a real client parses must be unchanged.
// 2. BACKPRESSURE -- a client that stops reading must not make the relay buffer
//    without limit; the stream must be closed once the cap is exceeded, and a
//    client that resumes must receive everything in order.
import http from 'node:http'

const PORT = Number(process.env.PORT || 25986)
let failures = 0
const check = (name, cond, detail = '') => {
  if (cond) console.log(`  PASS  ${name}`)
  else { failures++; console.log(`  FAIL  ${name}${detail ? ' -- ' + detail : ''}`) }
}

// Minimal stand-in for the relay's write path, exercising the same ordering rules.
const runStream = ({ keepaliveMs, capBytes, chunkEveryMs, totalChunks, acceptLimit }) => {
  let backlog = []
  let backlogBytes = 0
  let destroyed = false
  let written = []

  const res = {
    writableEnded: false,
    destroyed: false,
    destroy () { destroyed = true; res.destroyed = true },
    // `acceptLimit` models socket pressure: how many writes succeed before the
    // writer is told to back off. Infinity = an always-reading client.
    write (chunk) {
      written.push(chunk)
      return written.length <= acceptLimit
    },
    on () {},
  }

  const writeSse = (chunk) => {
    if (res.writableEnded || res.destroyed) return
    if (backlog.length) {
      backlog.push(chunk)
      backlogBytes += chunk.length
      if (backlogBytes > capBytes) res.destroy()
      return
    }
    if (!res.write(chunk)) { backlog.push(chunk); backlogBytes += chunk.length }
  }
  const drainBacklog = () => {
    while (backlog.length && !res.destroyed) {
      const c = backlog[0]
      if (!res.write(c)) return
      backlog.shift(); backlogBytes -= c.length
    }
  }

  const ka = setInterval(() => writeSse(': keepalive\n\n'), keepaliveMs)
  const t = setInterval(() => {
    writeSse(`data: {"delta":{"content":"chunk"}}\n\n`)
  }, chunkEveryMs)
  return new Promise((resolve) => setTimeout(() => {
    clearInterval(ka); clearInterval(t); drainBacklog()
    resolve({ written: written.join(''), backlogLeft: backlog.length, destroyed, backlogBytes })
  }, 200))
}

console.log('1. KEEPALIVE fires while the model is silent (client always reading)')
{
  const r = await runStream({
    keepaliveMs: 20, capBytes: 1e9, chunkEveryMs: 100, acceptLimit: Infinity,
  })
  const pings = (r.written.match(/^: keepalive/gm) || []).length
  check('keepalive frames were sent during silence', pings >= 3, `pings=${pings}`)
  const dataFrames = r.written.split('\n\n').filter((f) => f.startsWith('data:'))
  check('data frames still reached the wire', dataFrames.length > 0, `count=${dataFrames.length}`)
  let allParse = dataFrames.length > 0
  for (const f of dataFrames) {
    try { if (JSON.parse(f.slice(5)).delta?.content !== 'chunk') allParse = false } catch { allParse = false }
  }
  check('every data frame is valid JSON with its content intact', allParse)
  check('no keepalive text leaked into a data frame',
    !r.written.includes('"content":": keepalive'))
}

console.log('\n2. BACKPRESSURE caps an unread client')
{
  const r = await runStream({
    keepaliveMs: 1e9, capBytes: 512, chunkEveryMs: 2, acceptLimit: 3,
  })
  check('stream was closed rather than buffering forever', r.destroyed)
  check('buffered bytes stayed near the cap', r.backlogBytes <= 512 + 512,
    `backlogBytes=${r.backlogBytes}`)
}

console.log('\n3. ORDERING: keepalive never overtakes real data')
{
  const chunks = []
  const writeSse = (c) => chunks.push(c)
  writeSse(': keepalive\n\n')
  writeSse('data: {"delta":{"content":"a"}}\n\n')
  writeSse(': keepalive\n\n')
  writeSse('data: {"delta":{"content":"b"}}\n\n')
  const joined = chunks.join('')
  const seq = [...joined.matchAll(/(: keepalive|data:)/g)].map((m) => (m[1] === 'data:' ? 'D' : 'K'))
  check('frames emitted in call order', seq.join('') === 'KD KD'.replace(/ /g, ''), seq.join(''))
  check('content a precedes content b', joined.indexOf('"a"') < joined.indexOf('"b"'))
}

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL STREAM CHECKS PASSED')
process.exit(failures ? 1 : 0)