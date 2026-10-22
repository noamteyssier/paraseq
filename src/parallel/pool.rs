//! A worker count that can change while a parallel run is in flight.
//!
//! # Why
//!
//! `process_parallel*` fixes its worker count when it is called. That is the
//! right default, but it makes one class of decision unrecoverable: a caller
//! that splits a fixed CPU budget between parsing/processing threads and some
//! other consumer of the same budget — a parallel decompressor, say — has to
//! choose the split before reading a byte, and cannot correct it afterwards.
//!
//! A [`ThreadPool`] lets that caller start somewhere reasonable and converge on
//! evidence instead of committing up front.
//!
//! # Design: a fixed spawn set, parked when over target
//!
//! Every worker the pool can ever run is spawned once, up front, with a stable
//! index. A worker whose index is at or above the current target parks on a
//! condvar at a batch boundary; growing the target wakes it. **No worker ever
//! spawns another**, so the pool can never hold more workers than its ceiling
//! — that bound is structural, not enforced by counting, and there is no
//! spawn decision left to race on. (An earlier design spawned on demand when
//! running workers observed a shortfall; distinguishing "a peer left at EOF"
//! from "the pool is short" from a worker's viewpoint is inherently racy, and
//! under a small input the observed worker count could exceed the ceiling.)
//!
//! A parked worker holds no `RecordSet` — workers allocate theirs lazily on
//! first activation — so a worker that never runs costs one OS thread and
//! nothing else.
//!
//! # Cost
//!
//! An active worker checks one relaxed atomic once per *batch* — not per
//! record — and touches the mutex only when parking or being woken.

pub(crate) use super::pool_worker::process_parallel_pool_range;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Debug)]
struct Inner {
    /// Workers that should be running, across every share of this pool.
    ///
    /// Shared by all shares so that one `set_threads` reaches every reader a
    /// `Collection` is driving, without anything having to fan the change out.
    target: AtomicUsize,
    /// Hard ceiling on `target`.
    max: usize,
    /// Workers active across *every* share of this pool.
    ///
    /// A supervisor reads this both to normalise work against "threads
    /// running" and to learn when a shrink has taken effect -- typically before
    /// handing the freed CPUs to something else. That second use needs more
    /// than telemetry: once the count has fallen to a shrunk target it must
    /// *stay* there. So it is also the pool's admission count. Workers retire
    /// by index (at a batch boundary), but they are admitted by
    /// [`ThreadPool::claim_live`], under `gate`, only while this count is below
    /// the target; every increment happens there.
    total_live: AtomicUsize,
    /// Orders `set_threads` against a worker's check-then-park.
    ///
    /// The predicate a parked worker waits on reads atomics, but the *check
    /// then wait* must happen under this lock, and every state change that
    /// could unpark someone must notify while holding it. Changing state and
    /// notifying without the lock loses the wakeup when the change lands
    /// between a worker's check and its park — a bug class we have hit
    /// elsewhere, and one that a test with any other periodic notifier will
    /// never catch, because the next notify repairs the loss.
    gate: Mutex<()>,
    signal: Condvar,
    /// Test hook: sleep this long (µs) before a worker claims its live slot,
    /// to hold open the window between deciding to run and being counted...
    #[cfg(test)]
    activation_delay: std::sync::atomic::AtomicU64,
    /// ...except for this worker index, which claims immediately.
    #[cfg(test)]
    activation_exempt: AtomicUsize,
}

/// A shared, resizable worker count.
///
/// Cloning is cheap and every clone refers to the same pool. A `Collection`
/// driving several readers at once gives each one a [`share`](Self::share) of
/// the same pool, so the target the caller sets is a *total* rather than a
/// per-reader figure.
#[derive(Debug, Clone)]
pub struct ThreadPool {
    inner: Arc<Inner>,
    /// Workers running in *this* share. Only ever changed by a worker claiming
    /// or releasing its own slot, so it cannot drift from reality.
    live: Arc<AtomicUsize>,
    /// How many shares divide `inner.target`. One for an unshared pool.
    divisor: usize,
}

impl ThreadPool {
    /// A pool that never changes size. Values below one are clamped to one.
    pub fn new(threads: usize) -> Self {
        Self::with_max(threads, threads)
    }

    /// A pool starting at `threads` that may later grow as far as `max`.
    ///
    /// `max` bounds the fixed set of worker threads spawned for each parallel
    /// run. Workers above the current target park without allocating a
    /// `RecordSet` until the pool grows.
    pub fn with_max(threads: usize, max: usize) -> Self {
        let max = max.max(1);
        Self {
            inner: Arc::new(Inner {
                target: AtomicUsize::new(threads.clamp(1, max)),
                max,
                total_live: AtomicUsize::new(0),
                gate: Mutex::new(()),
                signal: Condvar::new(),
                #[cfg(test)]
                activation_delay: std::sync::atomic::AtomicU64::new(0),
                #[cfg(test)]
                activation_exempt: AtomicUsize::new(usize::MAX),
            }),
            live: Arc::new(AtomicUsize::new(0)),
            divisor: 1,
        }
    }

    /// A view of this pool for one of `ways` concurrent consumers.
    ///
    /// Each share tracks its own live workers but reads the same target, so a
    /// `set_threads` on any of them retargets all of them at once. The target
    /// is a total: `ways` shares of a 32-thread pool run 8 workers each.
    ///
    /// Sharing rather than handing the same pool to every reader is deliberate:
    /// each share spawns and parks its own slice of workers against its own
    /// slice of the target, so no reader can starve another of workers.
    pub fn share(&self, ways: usize) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            live: Arc::new(AtomicUsize::new(0)),
            divisor: self.divisor.saturating_mul(ways.max(1)).max(1),
        }
    }

    /// Change how many workers should be running, across every share.
    ///
    /// Takes effect within one batch per worker: growth wakes parked workers,
    /// and shrinking parks workers after they finish the batch in hand. Never
    /// interrupts work in progress, so no record is processed twice or
    /// dropped.
    ///
    /// A caller that hands the CPUs freed by a shrink to something else should
    /// wait for [`Self::total_live`] to reach the new target first: that is
    /// when the retiring workers have finished their batches, and from then on
    /// no worker is admitted while it would push the count back over. (This
    /// holds while the target is at least the number of shares running; below
    /// that every share still keeps one worker, see [`Self::share`].)
    pub fn set_threads(&self, threads: usize) {
        // Store under the gate so it is ordered against `claim_live`: a worker
        // being admitted either is admitted first, and is then visible in
        // `total_live` to whoever reads it after this returns, or is admitted
        // afterwards and sees the new target.
        // Lock-then-notify: see `Inner::gate` for why the lock is not optional.
        let _gate = self.inner.gate.lock().unwrap();
        self.inner
            .target
            .store(threads.clamp(1, self.inner.max), Ordering::Release);
        self.inner.signal.notify_all();
    }

    /// The current target, across every share.
    pub fn threads(&self) -> usize {
        self.inner.target.load(Ordering::Relaxed)
    }

    /// Workers running in this share.
    pub fn live(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    /// Workers running across every share of this pool.
    ///
    /// This is the figure a supervisor wants. [`Self::live`] reports only the
    /// share it is called on, and the pool a caller holds is the *parent*, whose
    /// own share never runs anything when a `Collection` splits it.
    pub fn total_live(&self) -> usize {
        self.inner.total_live.load(Ordering::Relaxed)
    }

    /// The ceiling set at construction, across every share.
    pub fn max_threads(&self) -> usize {
        self.inner.max
    }

    /// This share's slice of the target, never below one.
    pub(crate) fn share_target(&self) -> usize {
        (self.threads() / self.divisor).max(1)
    }

    /// This share's slice of the ceiling, never below one.
    pub(crate) fn share_max(&self) -> usize {
        (self.inner.max / self.divisor).max(1)
    }

    /// Block until `active()` is true, re-checking under the pool's gate.
    ///
    /// The closure is evaluated with the gate held; any state it reads must be
    /// changed only in combination with a locked notify (see [`Inner::gate`]).
    pub(crate) fn park_until(&self, mut active: impl FnMut() -> bool) {
        let mut gate = self.inner.gate.lock().unwrap();
        while !active() {
            gate = self.inner.signal.wait(gate).unwrap();
        }
    }

    /// Wake every parked worker so it re-checks its predicate.
    ///
    /// For state changes made outside the pool (end of input, a poisoned
    /// order gate) that must be able to unpark workers.
    pub(crate) fn wake_all(&self) {
        let _gate = self.inner.gate.lock().unwrap();
        self.inner.signal.notify_all();
    }

    /// Admit a parked or newly spawned worker, counting it live.
    ///
    /// Waits under the gate while the pool is full -- while workers retiring
    /// from a shrink still hold the slots -- and returns `false` without
    /// admitting if the worker's index is (now) over its share's target or
    /// `stop()` is true; the caller goes back round its loop and parks or
    /// exits.
    ///
    /// Both halves matter. Checking the target and counting the worker as two
    /// separate steps lets a shrink land between them: a supervisor sees
    /// `total_live` already at the new target, hands the freed CPUs away, and
    /// the worker then counts itself and runs a batch over budget. And
    /// admitting by index alone is not enough either: a worker under the new
    /// target that had not been counted yet (just spawned, or just woken by a
    /// growth) would start alongside the retirees still finishing their last
    /// batch, again after the supervisor has seen the shrink complete.
    pub(crate) fn claim_live(&self, worker_id: usize, mut stop: impl FnMut() -> bool) -> bool {
        #[cfg(test)]
        {
            let delay = self.inner.activation_delay.load(Ordering::Relaxed);
            if delay > 0 && worker_id != self.inner.activation_exempt.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_micros(delay));
            }
        }
        let mut gate = self.inner.gate.lock().unwrap();
        loop {
            if stop() || worker_id >= self.share_target() {
                return false;
            }
            if self.inner.total_live.load(Ordering::Relaxed) < self.live_cap() {
                self.enter_live();
                return true;
            }
            gate = self.inner.signal.wait(gate).unwrap();
        }
    }

    /// How many workers may be live across every share at once: the target,
    /// or one per share if there are more shares than that (each share keeps
    /// at least one worker, so no reader is starved).
    fn live_cap(&self) -> usize {
        self.threads().max(self.divisor)
    }

    /// Test hook: widen the window between a worker deciding to run and
    /// being counted live, for every worker but `exempt`.
    #[cfg(test)]
    pub(crate) fn set_activation_delay(&self, delay: std::time::Duration, exempt: Option<usize>) {
        self.inner
            .activation_delay
            .store(delay.as_micros() as u64, Ordering::Relaxed);
        self.inner
            .activation_exempt
            .store(exempt.unwrap_or(usize::MAX), Ordering::Relaxed);
    }

    /// Count this worker live. Workers are admitted through
    /// [`Self::claim_live`], which calls this under the gate.
    pub(crate) fn enter_live(&self) {
        self.live.fetch_add(1, Ordering::AcqRel);
        self.inner.total_live.fetch_add(1, Ordering::AcqRel);
    }

    /// Record this worker as parked or exited, and wake anyone waiting in
    /// [`Self::claim_live`] for the slot it frees. Parking and exiting are
    /// rare (resizes and end of input), so the locked notify costs nothing on
    /// the per-batch path.
    pub(crate) fn exit_live(&self) {
        self.live.fetch_sub(1, Ordering::AcqRel);
        self.inner.total_live.fetch_sub(1, Ordering::AcqRel);
        let _gate = self.inner.gate.lock().unwrap();
        self.inner.signal.notify_all();
    }
}
