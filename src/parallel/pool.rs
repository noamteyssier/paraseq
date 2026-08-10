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
    /// Purely telemetry: an external supervisor normalising work against
    /// "threads running" reads this. Nothing in the pool decides anything from
    /// it — activation is by stable worker index against the target, so this
    /// count can lag reality by a batch without consequence.
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
    /// A pool that never changes size — equivalent to passing `threads`
    /// directly to `process_parallel*`.
    pub fn new(threads: usize) -> Self {
        Self::with_max(threads, threads)
    }

    /// A pool starting at `threads` that may later grow as far as `max`.
    ///
    /// `max` bounds growth only; it costs nothing until asked for, because
    /// workers are spawned on demand.
    pub fn with_max(threads: usize, max: usize) -> Self {
        let max = max.max(1);
        Self {
            inner: Arc::new(Inner {
                target: AtomicUsize::new(threads.clamp(1, max)),
                max,
                total_live: AtomicUsize::new(0),
                gate: Mutex::new(()),
                signal: Condvar::new(),
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
    pub fn set_threads(&self, threads: usize) {
        self.inner
            .target
            .store(threads.clamp(1, self.inner.max), Ordering::Release);
        // Lock-then-notify: see `Inner::gate` for why the lock is not optional.
        let _gate = self.inner.gate.lock().unwrap();
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

    /// Record this worker as active. Telemetry only.
    pub(crate) fn enter_live(&self) {
        self.live.fetch_add(1, Ordering::AcqRel);
        self.inner.total_live.fetch_add(1, Ordering::AcqRel);
    }

    /// Record this worker as parked or exited. Telemetry only.
    pub(crate) fn exit_live(&self) {
        self.live.fetch_sub(1, Ordering::AcqRel);
        self.inner.total_live.fetch_sub(1, Ordering::AcqRel);
    }
}
