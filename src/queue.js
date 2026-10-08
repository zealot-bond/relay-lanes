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
  constructor ({ maxPending = 5000, claimLeaseMs = 90000, avoidTtlMs = 60000 } = {}) {
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
    // How long a lane stays marked as a bad retry target for an entry it just
    // failed. Long enough to try someone else, short enough that the mark cannot
    // strand work behind it.
    this.avoidTtlMs = avoidTtlMs
  }

  /**
 * Return claims whose lane never reported, so the work is not lost.
 *
 * An entry that has already finished, or whose client disconnected, is dropped
 * rather than requeued. Requeuing a dead entry is what created a zombie loop: a
 * lane would claim it, burn a full upstream call serving nobody, and the lease
 * would expire and hand it back again, forever, starving real requests.
 */
  /**
   * Refresh the lease on everything a lane is currently holding.
   *
   * Called from /lane/heartbeat and /lane/claim. A lane mid-way through a long
   * tool turn (or a 600s prefill) is heartbeating the whole time, so its claim is
   * provably still live and must not be handed to a second lane. Without this the
   * reaper requeued work that was still being served, and two lanes streamed the
   * same request into one client: interleaved text and scrambled tool arguments.
   */
  touchLane (laneId) {
    const now = Date.now()
    for (const entry of this.inflight.values()) {
      if (entry.claimedBy === laneId) entry.claimedAt = now
    }
  }

  /**
 * Return claims whose lane never reported, so the work is not lost.
 *
   * An entry that has already finished, or whose client disconnected, is dropped
   * rather than requeued. Requeuing a dead entry is what created a zombie loop: a
   * lane would claim it, burn a full upstream call serving nobody, and the lease
   * would expire and hand it back again, forever, starving real requests.
   *
   * `laneAlive` is supplied by the caller (which owns the lane registry): a claim
   * held by a lane that is still heartbeating is live no matter how long the lease
   * says, because claim age and lane liveness are different questions. Requeueing
   * work a live lane is still serving is what let two lanes stream into one
   * client at once -- interleaved text and doubled tool-call arguments.
   */
  reapStaleClaims (laneAlive = () => false) {
    const now = Date.now()
    const reaped = []
    for (const entry of this.inflight.values()) {
      if (!entry.claimedAt || now - entry.claimedAt <= this.claimLeaseMs) continue
      if (laneAlive(entry.claimedBy)) continue
      this.inflight.delete(entry.id)
      // Wake any dispatcher blocked on this entry: it is going back in the queue,
      // and leaving it waiting on a promise nobody will resolve is how a request
      // ended up held for the full ATTEMPT_HARD_MS with no lane assigned.
      // Say WHICH lane's claim was reaped. Without it the dispatcher guessed the
      // lane from its own hint, so the innocent lane was marked as failed while the
      // dead one stayed eligible.
      this._wake(entry, { kind: 'requeued', status: 0, ms: 0, data: null, laneId: entry.claimedBy || undefined })
      if (entry.done || entry.abandoned) continue
      // Text has already reached the client: serving the entry again from the start
      // would replay it into a stream that is already committed (observed: the client
      // received "hello " then "hello world"). The dispatcher finishes it as partial.
      if (entry.committed) continue
      if (!this.pending.includes(entry)) {
        entry.claimedBy = null
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

  /**
   * A lane claims work. Returns null when nothing is claimable.
   *
   * The oldest entry is preferred, EXCEPT one this lane is marked as having
   * already failed. Retrying a request on the egress IP that just rejected it
   * wastes a full upstream call: a 4xx is identical from the same IP and an
   * exhausted bucket is still exhausted. A lane the entry wants to avoid is
   * therefore skipped while any other entry is available; when it is the only
   * work left the entry is served anyway rather than stranded in the queue.
   */
  claim (laneId) {
    if (!this.pending.length) return null
    // Prefer the oldest entry this lane has NOT recently failed. A lane the
    // request already burned is the worst retry target: the same egress IP
    // reproduces the same 4xx, and an exhausted bucket is still exhausted.
    //
    // The mark EXPIRES (see avoidTtlMs) rather than being a permanent block. As a
    // hard filter it would strand work whenever every live lane had tried the
    // entry; with an expiry the worst case is that a lane retries one entry after
    // the window, exactly as it did before this existed.
    const now = Date.now()
    let idx = this.pending.findIndex((e) => !this._avoids(e, laneId, now))
    if (idx === -1) idx = 0
    const [entry] = this.pending.splice(idx, 1)
    entry.claimedBy = laneId
    entry.claimedAt = Date.now()
    entry.attempts++
    this.inflight.set(entry.id, entry)
    return entry
  }

  /** Has this lane failed this entry recently enough that it should not retry? */
  _avoids (entry, laneId, now) {
    const at = entry.avoid?.get?.(laneId)
    if (!at) return false
    if (now - at > this.avoidTtlMs) {
      entry.avoid.delete(laneId)
      return false
    }
    return true
  }

  /**
   * Wake a dispatcher parked on this entry, if there is one.
   *
   * `deliverResult` in server.js owns the resolver; the queue only knows the entry
   * object, so the wake is expressed as a queued result the dispatcher will pick
   * up. That keeps one mechanism instead of two (and the old two could disagree,
   * which hung a request with a result sitting in a field nobody read).
   */
  _wake (entry, result) {
    if (typeof entry.resultWaiter === 'function') {
      const wake = entry.resultWaiter
      entry.resultWaiter = null
      wake(result)
    }
  }

  /** Mark a lane as a bad retry target for an entry, for a bounded window. */
  avoidLane (entry, laneId) {
    if (!entry || !laneId) return
    if (!entry.avoid) entry.avoid = new Map()
    entry.avoid.set(laneId, Date.now())
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
    // The client promise must be settled even when the entry was already marked
    // done by abandon(). Returning early here left handleChat parked forever, so
    // `streamers.delete()` never ran and every aborted streaming request leaked
    // its closure for the life of the process.
    const firstSettlement = !entry.done
    entry.done = true
    this._wake(entry, { kind: 'finished' })
    entry.resolve?.(result)
    if (firstSettlement) this.emit('done', { entry, result })
  }

  /** Drop an entry whose client is gone, so no lane spends time on it. */
  abandon (entry) {
    if (!entry) return
    entry.abandoned = true
    entry.done = true
    this.inflight.delete(entry.id)
    const at = this.pending.indexOf(entry)
    if (at !== -1) this.pending.splice(at, 1)
    // Settle the dispatcher. Without this it stays parked on its attempt promise
    // until ATTEMPT_HARD_MS expires, and handleChat never gets to delete its
    // streamer entry, so every abandoned request leaks one closure for the life
    // of the process.
    this._wake(entry, { kind: 'abandoned', status: 0, ms: 0, data: null })
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
    // Never let the same entry sit in `pending` twice. Two callers can requeue
    // (the reaper hands work back while the dispatcher is also retrying), and a
    // duplicated entry is claimed by two lanes at once -- both stream into the
    // same client, which is interleaved text and scrambled tool arguments.
    if (this.pending.includes(entry)) return true
    this._wake(entry, { kind: 'requeued', status: 0, ms: 0, data: null, laneId: entry.claimedBy || undefined })
    entry.claimedBy = null
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
    // Ids whose egress bucket is spent. A retired lane must never serve again:
    // the worker re-registers itself when the relay says "lane not registered"
    // (which is how it recovers from a relay restart), and without this it would
    // walk straight back in and spend its dead IP on more work.
    this.tombstones = new Map()  // laneId -> retiredAt
    this.maxLanes = maxLanes
  }

  register (laneId, meta = {}) {
    const dead = this.tombstones.get(laneId)
    if (dead) return { retired: true, retiredAt: dead }
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
    // Never let spread-over-meta reinstate a tombstoned status.
    lane.status = 'idle'
    this.lanes.set(laneId, lane)
    this.emit('lane:up', lane)
    return lane
  }

  /** Forget lanes that have stopped reporting. Returns the ids removed. */
  /**
 * Forget lanes that stopped reporting.
 *
 * `staleMs` must not exceed the window dispatch uses to consider a lane live.
 * It used to default to 300s while selection used 45s, so after a mass lane death
 * (lanes start in bursts and reach MAX_LIFETIME_S together) the pool reported zero
 * live lanes while every registration slot was still held: /lane/register returned
 * "lane capacity reached" for five minutes, the new runners exited immediately, and
 * the orchestrator burned its dispatch budget on lanes that could never start.
 * Generous enough to cover a heartbeat gap, short enough to free the slot.
 */
  prune (staleMs = 45000) {
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
    // Tombstone even a lane that is already gone from the live map: a result can
    // arrive after the record was pruned, and the retirement must still stick.
    this.tombstones.set(laneId, Date.now())
    if (this.tombstones.size > 2000) {
      // Keep the map bounded without losing recent retirements.
      const cutoff = Date.now() - 24 * 3600000
      for (const [id, at] of this.tombstones) {
        if (at > cutoff) break
        this.tombstones.delete(id)
      }
    }
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