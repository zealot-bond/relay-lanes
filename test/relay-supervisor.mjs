// Tests scripts/relay-supervisor.sh with stub relay and tunnel commands and a local
// stand-in for the GitHub API. Nothing here reaches the network: GH_API points at this
// process, so no real workflow is ever dispatched.
//
//   node test/relay-supervisor.mjs
import { spawn, execFileSync } from 'node:child_process'
import http from 'node:http'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const SUPERVISOR = path.join(here, '..', 'scripts', 'relay-supervisor.sh')

let failures = 0
const check = (name, ok, detail = '') => {
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' -- ' + detail : ''}`)
  if (!ok) failures++
}

function fakeGitHub () {
  const posts = []
  const srv = http.createServer((req, res) => {
    let body = ''
    req.on('data', (c) => { body += c })
    req.on('end', () => {
      posts.push({ method: req.method, url: req.url, auth: req.headers.authorization, body, at: Date.now() })
      res.writeHead(204).end()
    })
  })
  return new Promise((resolve) => srv.listen(0, '127.0.0.1', () => resolve({
    url: `http://127.0.0.1:${srv.address().port}`,
    posts,
    close: () => srv.close(),
  })))
}

function supervise (env) {
  return new Promise((resolve) => {
    const t0 = Date.now()
    const child = spawn('bash', [SUPERVISOR], {
      env: { PATH: process.env.PATH, ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let out = ''
    child.stdout.on('data', (d) => { out += d })
    child.stderr.on('data', (d) => { out += d })
    child.on('exit', (code) => resolve({ code, out, ms: Date.now() - t0, t0 }))
  })
}

const running = (marker) => execFileSync('ps', ['-eo', 'args'], { encoding: 'utf8' }).includes(marker)

const base = (gh) => ({
  CF_TUNNEL_TOKEN: 'stub-tunnel-token',
  GH_TOKEN: 'gh-test-token',
  GITHUB_REPOSITORY: 'owner/repo',
  GH_API: gh.url,
  GH_REF: 'main',
  RELAY_WORKFLOW: 'relay.yml',
  POLL_S: '1',
})

console.log('1. a generation serves its full time and hands over once')
{
  const gh = await fakeGitHub()
  const r = await supervise({
    ...base(gh), RUN_FOR_S: '6', HANDOVER_LEAD_S: '2',
    RELAY_CMD: 'sleep 613.11', TUNNEL_CMD: 'sleep 614.22',
  })
  gh.close()
  check('exits 0 at the end of its time', r.code === 0, `code=${r.code}`)
  check('ends after its full RUN_FOR_S, not before', r.ms >= 6000 && r.ms < 8000, `${r.ms}ms`)
  check('dispatches exactly one successor', gh.posts.length === 1, `posts=${gh.posts.length}`)
  const p = gh.posts[0]
  check('dispatches relay.yml on main in this repository',
    p?.url === '/repos/owner/repo/actions/workflows/relay.yml/dispatches' && p.body === '{"ref":"main"}',
    p ? `${p.method} ${p.url} ${p.body}` : 'none')
  check('authenticates with the token it was given', p?.auth === 'Bearer gh-test-token')
  const lead = p ? p.at - r.t0 : -1
  check('hands over HANDOVER_LEAD_S before the end, not at it', lead >= 3000 && lead <= 5500, `at ${lead}ms`)
  check('stops the relay and the tunnel when done',
    !running('sleep 613.11') && !running('sleep 614.22'))
}

console.log('2. a relay that dies early dispatches nothing, so a broken config cannot loop')
{
  const gh = await fakeGitHub()
  const r = await supervise({
    ...base(gh), RUN_FOR_S: '60', HANDOVER_LEAD_S: '5',
    RELAY_CMD: 'sleep 1', TUNNEL_CMD: 'sleep 615.33',
  })
  gh.close()
  check('exits non-zero', r.code === 1, `code=${r.code}`)
  check('dispatches no successor', gh.posts.length === 0, `posts=${gh.posts.length}`)
  check('does not wait out its full time', r.ms < 15000, `${r.ms}ms`)
  check('says which part died', /relay exited/.test(r.out))
  check('says that the schedule takes over', /no successor dispatched/.test(r.out))
  check('stops the tunnel it no longer needs', !running('sleep 615.33'))
}

console.log('3. a tunnel that dies early is treated the same way')
{
  const gh = await fakeGitHub()
  const r = await supervise({
    ...base(gh), RUN_FOR_S: '60', HANDOVER_LEAD_S: '5',
    RELAY_CMD: 'sleep 616.44', TUNNEL_CMD: 'sleep 1',
  })
  gh.close()
  check('exits non-zero', r.code === 1, `code=${r.code}`)
  check('dispatches no successor', gh.posts.length === 0, `posts=${gh.posts.length}`)
  check('says which part died', /tunnel connector exited/.test(r.out))
  check('stops the relay it no longer has a tunnel for', !running('sleep 616.44'))
}

console.log('4. a failed dispatch is reported, and does not stop the generation')
{
  const r = await supervise({
    ...base({ url: 'http://127.0.0.1:9' }), RUN_FOR_S: '3', HANDOVER_LEAD_S: '1',
    RELAY_CMD: 'sleep 617.55', TUNNEL_CMD: 'sleep 618.66',
  })
  check('still exits 0 at the end of its time', r.code === 0, `code=${r.code}`)
  check('reports the failed dispatch as an error', /could not dispatch the successor/.test(r.out))
}

console.log('5. refuses to start without a tunnel token')
{
  const gh = await fakeGitHub()
  const { CF_TUNNEL_TOKEN, ...rest } = base(gh)
  const r = await supervise({ ...rest, RUN_FOR_S: '3', RELAY_CMD: 'sleep 1', TUNNEL_CMD: 'sleep 1' })
  gh.close()
  check('exits non-zero', r.code !== 0, `code=${r.code}`)
  check('names the missing variable', /CF_TUNNEL_TOKEN/.test(r.out))
  check('dispatches nothing', gh.posts.length === 0)
}

console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL RELAY-SUPERVISOR CHECKS PASSED')
process.exit(failures ? 1 : 0)
