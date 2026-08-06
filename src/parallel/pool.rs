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
    /// Workers that should be running.
    target: AtomicUsize,
    /// Workers actually running. Only ever changed by a worker claiming or
    /// releasing its own slot, so it cannot drift from reality.
    live: AtomicUsize,
    /// Hard ceiling. Bounds `target`, so a caller cannot ask for unbounded
    /// threads by accident.
    max: usize,
}

/// A shared, resizable worker count.
///
/// Cloning is cheap and every clone refers to the same pool.
#[derive(Debug, Clone)]
pub struct ThreadPool {
    inner: Arc<Inner>,
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
                live: AtomicUsize::new(0),
                max,
            }),
        }
    }

    /// Change how many workers should be running.
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

    /// The current target.
    pub fn threads(&self) -> usize {
        self.inner.target.load(Ordering::Relaxed)
    }

    /// Workers actually running now.
    pub fn live(&self) -> usize {
        self.inner.live.load(Ordering::Relaxed)
    }

    /// The ceiling set at construction.
    pub fn max_threads(&self) -> usize {
        self.inner.max
    }

    /// Claim a slot for a new worker, if the pool is short of its target.
    ///
    /// A CAS rather than a load-then-store because several workers may notice
    /// the same shortfall at once, and two of them spawning for one slot would
    /// overshoot the target.
    pub(crate) fn try_claim_slot(&self) -> bool {
        let mut live = self.inner.live.load(Ordering::Relaxed);
        loop {
            let target = self.inner.target.load(Ordering::Relaxed);
            if live >= target || live >= self.inner.max {
                return false;
            }
            match self.inner.live.compare_exchange_weak(
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
        let mut live = self.inner.live.load(Ordering::Relaxed);
        loop {
            if live <= self.inner.target.load(Ordering::Relaxed) || live <= 1 {
                return false;
            }
            match self.inner.live.compare_exchange_weak(
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
        self.inner.live.fetch_sub(1, Ordering::AcqRel);
    }
}
