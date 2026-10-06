// Work queue plus lane registry.
//
// Shape of the system:
//   client  --HTTP-->  relay server  --queue-->  lanes (GitHub Actions jobs)
//   lane  --claim-->  queue         <--result--  lane
//
// A lane is a GitHub Actions job. GitHub allows 20 concurrent jobs per free
// account, so the design targets exactly 20 live lanes. When a job exits, the
// orchestrator starts the next queued one, which is why the pool self-heals.
//
// Nothing here treats a 429 as an error the client should see: a lane that
// reports exhaustion is retired, not returned as a failure. The request is held
// and re-dispatched to another lane.

import { EventEmitter } from 'node:events'

let seq = 0
const nextId = () => `job-${Date.now().toString(36)}-${(seq++).toString(36)}`

export class WorkQueue extends EventEmitter {
  constructor ({ maxPending = 5000, claimLeaseMs = 90000 } = {}) {
    super()
    // Each waiting lane registers one 'work' listener, so a full pool legitimately
    // exceeds Node's default cap of 10 and warns about a leak that does not
    // exist. Every listener still removes itself on wake or timeout.
    this.setMaxListeners(0)

    this.pending = []          // FIFO of waiting jobs
    this.inflight = new Map()  // id -> entry currently claimed by a lane
    this.maxPending = maxPending
    // A lane can die mid-claim (runner killed, job timeout, network drop). Its
    // claim is reclaimed after this long so one dead lane cannot strand work or
    // make a healthy lane look busy.
    this.claimLeaseMs = claimLeaseMs
  }

  /**
 * Return claims whose lane never reported, so the work is not lost.
 *
 * An entry that has already finished, or whose client disconnected, is dropped
 * rather than requeued. Requeuing a dead entry is what created a zombie loop: a
 * lane would claim it, burn a full upstream call serving nobody, and the lease
 * would expire and hand it back again, forever, starving real requests.
 */
reapStaleClaims () {
    const now = Date.now()
    const reaped = []
    for (const entry of this.inflight.values()) {
      if (entry.claimedAt && now - entry.claimedAt > this.claimLeaseMs) {
        this.inflight.delete(entry.id)
        entry.pendingResolve = null
        if (entry.done || entry.abandoned) continue
        this.pending.push(entry)
        reaped.push(entry)
      }
    }
    if (reaped.length) this.emit('work')
    return reaped
  }

  /**
   * Enqueue work. `job` is the caller's object and IS the queue entry: the id
   * and internal fields are attached to it rather than wrapping it, because the
   * caller holds a reference and must be able to resolve the same object a lane
   * later reports on.
   */
  push (job) {
    if (this.pending.length >= this.maxPending) {
      return { ok: false, reason: 'queue_full' }
    }
    job.id = job.id || nextId()
    job.enqueuedAt = job.enqueuedAt || Date.now()
    job.attempts = 0
    this.pending.push(job)
    this.emit('queued', job)
    // Wake any lane blocked in claim().
    this.emit('work')
    return { ok: true, id: job.id }
  }

  /** A lane claims the oldest job. Returns null when the queue is empty. */
  claim (laneId) {
    const entry = this.pending.shift()
    if (!entry) return null
    entry.claimedBy = laneId
    entry.claimedAt = Date.now()
    entry.attempts++
    this.inflight.set(entry.id, entry)
    return entry
  }

  complete (jobId, result) {
    const entry = this.inflight.get(jobId)
    if (!entry) return false
    this.inflight.delete(jobId)
    entry.resolve?.(result)
    this.emit('done', { entry, result })
    return true
  }

  /**
   * Retire an entry from the queue exactly once and settle its client.
   *
   * Every dispatch ends here, whatever the outcome. Centralising removal is what
   * stops an entry being left in `pending` (never claimable by a dispatcher, so it
   * would only be served and requeued) or left in `inflight` (reaped and
   * requeued on lease expiry, which is the zombie loop).
   */
  finish (entry, result) {
    if (!entry) return
    this.inflight.delete(entry.id)
    const at = this.pending.indexOf(entry)
    if (at !== -1) this.pending.splice(at, 1)
    if (entry.done) return
    entry.done = true
    entry.pendingResolve = null
    entry.resolve?.(result)
    this.emit('done', { entry, result })
  }

  /** Drop an entry whose client is gone, so no lane spends time on it. */
  abandon (entry) {
    if (!entry) return
    entry.abandoned = true
    entry.done = true
    entry.pendingResolve = null
    this.inflight.delete(entry.id)
    const at = this.pending.indexOf(entry)
    if (at !== -1) this.pending.splice(at, 1)
  }

  /**
   * Return an in-flight entry to the pending list.
   *
   * Needed because a lane that reports exhaustion consumes the claim: the entry
   * leaves `inflight` as soon as the result arrives, so a retry loop that just
   * waits on the same entry would block forever. Requeueing returns the work to
   * a lane that can actually serve it.
   */
  requeue (entry, { front = true } = {}) {
    if (!entry) return false
    // Never resurrect finished or abandoned work.
    if (entry.done || entry.abandoned) return false
    this.inflight.delete(entry.id)
    entry.pendingResolve = null
    if (front) this.pending.unshift(entry)
    else this.pending.push(entry)
    this.emit('work')
    return true
  }

  /** Wait for work, up to timeoutMs. Resolves an entry or null. */
  async take (laneId, timeoutMs = 5000) {
    const immediate = this.claim(laneId)
    if (immediate) return immediate
    return new Promise((resolve) => {
      const timer = setTimeout(() => { this.off('work', onWork); resolve(this.claim(laneId)) }, timeoutMs)
      const onWork = () => {
        clearTimeout(timer)
        this.off('work', onWork)
        resolve(this.claim(laneId))
      }
      this.once('work', onWork)
    })
  }

  stats () {
    return {
      pending: this.pending.length,
      inflight: this.inflight.size,
      total: seq,
    }
  }
}

export class LaneRegistry extends EventEmitter {
  constructor ({ maxLanes = 20 } = {}) {
    super()
    this.lanes = new Map()   // laneId -> lane record (live)
    this.retired = []        // history, so bucket exhaustion stays countable
    this.maxLanes = maxLanes
  }

  register (laneId, meta = {}) {
    const existing = this.lanes.get(laneId)
    if (existing) {
      existing.lastSeen = Date.now()
      existing.heartbeats++
      return existing
    }
    // Prune before enforcing the cap: runners that died without reporting would
    // otherwise hold slots forever and make the pool look permanently full.
    this.prune()
    if (this.lanes.size >= this.maxLanes) return null
    const lane = {
      id: laneId,
      registeredAt: Date.now(),
      lastSeen: Date.now(),
      heartbeats: 0,
      served: 0,
      failed: 0,
      empty: 0,
      limited: 0,
      timeouts: 0,
      avgMs: 0,
      status: 'idle',        // idle | busy | exhausted | dead
      ...meta,
    }
    this.lanes.set(laneId, lane)
    this.emit('lane:up', lane)
    return lane
  }

  /** Forget lanes that have stopped reporting. Returns the ids removed. */
  prune (staleMs = 300000) {
    const now = Date.now()
    const gone = []
    for (const [id, lane] of this.lanes) {
      if (now - lane.lastSeen > staleMs) {
        this.lanes.delete(id)
        lane.status = 'dead'
        gone.push(id)
        this.emit('lane:down', lane)
      }
    }
    return gone
  }

  heartbeat (laneId) {
    const lane = this.lanes.get(laneId)
    if (!lane) return null
    lane.lastSeen = Date.now()
    lane.heartbeats++
    return lane
  }

  record (laneId, outcome, ms) {
    const lane = this.lanes.get(laneId)
    if (!lane) return null
    lane.lastSeen = Date.now()
    lane.avgMs = lane.avgMs ? Math.round(lane.avgMs * 0.7 + ms * 0.3) : ms
    if (outcome === 'ok') { lane.served++; lane.status = 'idle' }
    else if (outcome === 'limited') { lane.limited++; lane.status = 'exhausted' }
    else if (outcome === 'timeout') { lane.timeouts++; lane.status = 'idle' }
    else if (outcome === 'empty') { lane.empty++; lane.status = 'idle' }
    else { lane.failed++; lane.status = 'idle' }
    return lane
  }

  /**
   * Drop a lane that reported exhaustion so the orchestrator replaces it.
   * The record is archived rather than discarded: the cumulative counters are
   * how you tell whether buckets are being burned faster than they refill.
   */
  retire (laneId, reason) {
    const lane = this.lanes.get(laneId)
    if (!lane) return false
    lane.status = 'retired'
    lane.retiredReason = reason
    lane.retiredAt = Date.now()
    this.lanes.delete(laneId)
    this.retired.unshift(lane)
    if (this.retired.length > 200) this.retired.length = 200
    this.emit('lane:down', lane)
    return true
  }

  live () {
    return [...this.lanes.values()]
  }

  /** Lanes that look alive: heartbeating within staleMs. */
  liveLanes (staleMs = 45000) {
    const now = Date.now()
    return this.live().filter((l) => now - l.lastSeen < staleMs)
  }

  stats () {
    const all = this.live()
    const gone = this.retired
    const sum = (arr, k) => arr.reduce((a, l) => a + (l[k] || 0), 0)
    return {
      lanes: all.length,
      maxLanes: this.maxLanes,
      stale: all.filter((l) => Date.now() - l.lastSeen > 45000).length,
      available: all.filter((l) => l.status !== 'exhausted').length,
      exhausted: all.filter((l) => l.status === 'exhausted').length,
      retired: gone.length,
      served: sum(all, 'served') + sum(gone, 'served'),
      limited: sum(all, 'limited') + sum(gone, 'limited'),
      empty: sum(all, 'empty') + sum(gone, 'empty'),
      timeouts: sum(all, 'timeouts') + sum(gone, 'timeouts'),
      failed: sum(all, 'failed') + sum(gone, 'failed'),
      avgMs: all.length ? Math.round(sum(all, 'avgMs') / all.length) : 0,
    }
  }
}