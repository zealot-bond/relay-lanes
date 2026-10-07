// Fingerprint of the code that runs on the LANE side.
//
// Why this exists: the relay (in the jar) and the lanes (on GitHub runners) are
// deployed by two different mechanisms. The jar is uploaded by hand; the runners
// check out a git repository. For a long stretch fixes were built into the jar and
// the relay reported its own build id, while the runners kept executing code from
// before the fixes -- so the relay looked current and the tool-call corruption
// (names doubled to "bashbash", arguments emitted twice) kept happening. Nothing
// could say which code a lane was actually running.
//
// The hash covers the files a lane executes. Each lane reports it on registration;
// the relay hashes its own bundled copies of the same files and compares. They are
// the same source tree, so they match exactly when the runner repo is in step with
// the jar, and differ whenever it is not.

import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

// Every module a lane imports, directly or transitively. Keep in step with
// worker.js: a file missing here is a file whose staleness goes undetected.
export const LANE_FILES = [
  'codeversion.js',
  'config.js',
  'fingerprint.js',
  'lane.js',
  'models.js',
  'toolmerge.js',
  'worker.js',
]

const here = path.dirname(fileURLToPath(import.meta.url))

/** Short hash of the lane-side source in `dir` (defaults to this module's dir). */
export function computeCodeVersion (dir = here) {
  const h = crypto.createHash('sha256')
  for (const name of LANE_FILES) {
    let bytes
    try { bytes = fs.readFileSync(path.join(dir, name)) } catch { bytes = Buffer.from('<missing>') }
    // The name is mixed in so moving content between files changes the hash.
    h.update(name); h.update('\0'); h.update(bytes); h.update('\0')
  }
  return h.digest('hex').slice(0, 12)
}

export const CODE_VERSION = computeCodeVersion()
