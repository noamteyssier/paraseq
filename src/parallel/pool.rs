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
//! # Cost
//!
//! Workers are spawned on demand, not pre-spawned and parked. Pre-spawning is
//! simpler but allocates a `RecordSet` per worker up front — at large batch
//! sizes that is megabytes each, paid whether or not the worker ever runs.
//!
//! In exchange, a worker checks two relaxed atomics once per *batch* — not per
//! record — and takes neither branch unless the target has actually moved.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct Inner {
    /// Workers that should be running, across every share of this pool.
    ///
    /// Shared by all shares so that one `set_threads` reaches every reader a
    /// `Collection` is driving, without anything having to fan the change out.
    target: AtomicUsize,
    /// Hard ceiling on `target`.
    max: usize,
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
    /// Sharing rather than handing the same pool to every reader is deliberate.
    /// A single pool would let the first reader claim every slot, leaving the
    /// rest to spawn nothing — and since workers are only spawned *by* running
    /// workers, those readers would never start and their input would be
    /// silently skipped.
    pub fn share(&self, ways: usize) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            live: Arc::new(AtomicUsize::new(0)),
            divisor: self.divisor.saturating_mul(ways.max(1)).max(1),
        }
    }

    /// Change how many workers should be running, across every share.
    ///
    /// Takes effect within one batch per worker: growth spawns as running
    /// workers notice the gap, and shrinking retires workers after they finish
    /// the batch in hand. Never interrupts work in progress, so no record is
    /// processed twice or dropped.
    pub fn set_threads(&self, threads: usize) {
        self.inner
            .target
            .store(threads.clamp(1, self.inner.max), Ordering::Relaxed);
    }

    /// The current target, across every share.
    pub fn threads(&self) -> usize {
        self.inner.target.load(Ordering::Relaxed)
    }

    /// Workers running in this share.
    pub fn live(&self) -> usize {
        self.live.load(Ordering::Relaxed)
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

    /// Claim a slot for a new worker, if the pool is short of its target.
    ///
    /// A CAS rather than a load-then-store because several workers may notice
    /// the same shortfall at once, and two of them spawning for one slot would
    /// overshoot the target.
    pub(crate) fn try_claim_slot(&self) -> bool {
        let mut live = self.live.load(Ordering::Relaxed);
        loop {
            if live >= self.share_target() || live >= self.share_max() {
                return false;
            }
            match self.live.compare_exchange_weak(
                live,
                live + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => live = actual,
            }
        }
    }

    /// Release this worker's slot if the pool is over its target.
    ///
    /// Symmetric to [`Self::try_claim_slot`]: the CAS is what stops every
    /// surplus worker retiring at once when the target drops by one.
    pub(crate) fn try_release_slot(&self) -> bool {
        let mut live = self.live.load(Ordering::Relaxed);
        loop {
            if live <= self.share_target() || live <= 1 {
                return false;
            }
            match self.live.compare_exchange_weak(
                live,
                live - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => live = actual,
            }
        }
    }

    /// Release unconditionally, for a worker leaving because the input ended.
    pub(crate) fn release_slot(&self) {
        self.live.fetch_sub(1, Ordering::AcqRel);
    }
}
