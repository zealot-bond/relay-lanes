// One-time setup: create the repo, push the relay + lane workflow, then start
// the orchestrator. This is the "paste your token and it does everything" path.
//
//   node src/setup.js --token ghp_xxx [--repo my-org/my-repo] [--lanes 20]
//
// Safe to re-run: it is idempotent, and it never prints the token.

import fs from 'node:fs'
import path from 'node:path'
import https from 'node:https'
import { fileURLToPath } from 'node:url'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const ROOT = path.resolve(HERE, '..')
const API = 'https://api.github.com'

const args = process.argv.slice(2)
const arg = (name, fallback) => {
  const i = args.indexOf(`--${name}`)
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback
}

const TOKEN = process.env.GH_TOKEN || arg('token')
const REPO = arg('repo') || process.env.GH_REPO
const LANES = Number(arg('lanes') || 20)

if (!TOKEN) {
  console.error('usage: node src/setup.js --token ghp_xxx [--repo owner/name] [--lanes 20]')
  process.exit(1)
}

function api (apiPath, { method = 'GET', body, raw = false } = {}) {
  return new Promise((resolve, reject) => {
    const data = body === undefined ? null : JSON.stringify(body)
    const req = https.request(API + apiPath, {
      method,
      headers: {
        Accept: 'application/vnd.github+json',
        'User-Agent': 'relay-setup',
        Authorization: `Bearer ${TOKEN}`,
        ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {}),
      },
      timeout: 30000,
    }, (res) => {
      let out = ''
      res.on('data', (c) => { out += c; })
      res.on('end', () => {
        if (res.statusCode >= 300 && res.statusCode !== 422) {
          return reject(new Error(`HTTP ${res.statusCode}: ${out.slice(0, 300)}`))
        }
        if (raw) return resolve(out)
        try { resolve(out ? JSON.parse(out) : null) } catch { resolve(out) }
      })
    })
    req.on('error', reject)
    req.setTimeout(30000, () => req.destroy(new Error('timeout')))
    if (data) req.write(data)
    req.end()
  })
}

async function whoami () {
  const u = await api('/user')
  return u.login
}

async function ensureRepo (name) {
  // Try to create; 422 means it already exists, which is fine.
  try {
    const r = await api('/user/repos', {
      method: 'POST',
      body: { name, private: false, auto_init: true, description: 'GitHub Actions lane pool for OpenCode Zen free models' },
    })
    console.log(`[setup] created repo ${r.full_name}`)
    return r.full_name
  } catch (e) {
    if (/already exists/i.test(e.message)) {
      console.log(`[setup] repo ${name} already exists`)
      return name
    }
    throw e
  }
}

async function putFile (repo, filePath, content, message) {
  const b64 = Buffer.from(content).toString('base64')
  try {
    const cur = await api(`/repos/${repo}/contents/${filePath}`)
    await api(`/repos/${repo}/contents/${filePath}`, {
      method: 'PUT',
      body: { message, content: b64, sha: cur.sha, branch: 'main' },
    })
    console.log(`[setup] updated ${filePath}`)
  } catch {
    await api(`/repos/${repo}/contents/${filePath}`, {
      method: 'PUT',
      body: { message, content: b64, branch: 'main' },
    })
    console.log(`[setup] created ${filePath}`)
  }
}

async function main () {
  const login = await whoami()
  console.log(`[setup] authenticated as ${login}`)

  const repoName = REPO ? REPO.split('/')[1] : 'opencode-relay-lanes'
  const repo = await ensureRepo(repoName)
  console.log(`[setup] target repo: ${repo}`)

  const files = [
    ['src/fingerprint.js', 'src/fingerprint.js'],
    ['src/models.js', 'src/models.js'],
    ['src/queue.js', 'src/queue.js'],
    ['src/lane.js', 'src/lane.js'],
    ['src/worker.js', 'src/worker.js'],
    ['src/server.js', 'src/server.js'],
    ['src/orchestrator.js', 'src/orchestrator.js'],
    ['.github/workflows/lane.yml', '.github/workflows/lane.yml'],
    ['package.json', 'package.json'],
    ['README.md', 'README.md'],
  ]

  for (const [from, to] of files) {
    const abs = path.join(ROOT, from)
    if (!fs.existsSync(abs)) { console.log(`[setup] skip missing ${from}`); continue }
    await putFile(repo, to, fs.readFileSync(abs, 'utf8'), `relay: ${to}`)
  }

  // Repo secret so lanes can authenticate to the relay.
  // NOTE: the relay must be reachable from the runners, so it cannot be
  // 127.0.0.1 on this machine. Set RELAY_URL to a public host when self-hosting.
  try {
    await api(`/repos/${repo}/actions/secrets/RELAY_TOKEN`, {
      method: 'PUT',
      body: { encrypted_value: 'set-locally', key_id: '', name: 'RELAY_TOKEN' },
    })
  } catch { /* secrets need libsodium; documented below instead */ }

  console.log('')
  console.log('[setup] done. next steps:')
  console.log(`  1. start the relay:      RELAY_TOKEN=$(openssl rand -hex 16) npm start`)
  console.log(`  2. expose it to runners: RELAY_URL must be reachable from GitHub Actions`)
  console.log(`  3. start lanes:          LANES_TARGET=${LANES} node src/orchestrator.js`)
  console.log('')
  console.log('  The relay token is NOT pushed by this script. Create the repo secret')
  console.log('  yourself: gh secret set RELAY_TOKEN --repo ' + repo)
  console.log('  and set the same value as RELAY_TOKEN when starting the relay.')
}

main().catch((e) => { console.error('[setup] failed:', e.message); process.exit(1) })