use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::{Condvar, Mutex};
use smallvec::SmallVec;

use crate::{Record, MAX_ARITY};

use super::error::Result;
use super::processor::{MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor};

/// Coordinates worker threads so a per-batch side effect (in practice,
/// `on_batch_complete`) runs in the same order batches were claimed from the
/// underlying stream, even though the batches themselves may finish
/// out of order.
///
/// `next` holds the stream position of the batch allowed to proceed next.
/// A worker blocks in [`OrderGate::wait_turn`] until its own batch's start
/// position is reached, then calls [`OrderGate::advance`] to hand off to
/// whichever batch comes after it.
pub(crate) struct OrderGate {
    next: Mutex<usize>,
    cond: Condvar,
    poisoned: AtomicBool,
}

impl OrderGate {
    pub(crate) fn new() -> Self {
        Self {
            next: Mutex::new(0),
            cond: Condvar::new(),
            poisoned: AtomicBool::new(false),
        }
    }

    /// Blocks until `batch_start` is the next expected position, or the
    /// gate has been poisoned by a failing worker elsewhere in the pool.
    ///
    /// Poisoning only happens once some worker has already failed, at which
    /// point the overall call is going to return that error regardless, so
    /// releasing waiters without re-checking ordering is safe.
    pub(crate) fn wait_turn(&self, batch_start: usize) {
        let mut next = self.next.lock();
        while *next != batch_start && !self.poisoned.load(Ordering::Acquire) {
            self.cond.wait(&mut next);
        }
    }

    /// Advances the expected position to (at least) `batch_end` and wakes
    /// any threads waiting for their turn. Uses `max` rather than a plain
    /// assignment because batches skipped for being before a requested
    /// offset advance the gate without waiting, and may race each other.
    pub(crate) fn advance(&self, batch_end: usize) {
        let mut next = self.next.lock();
        if batch_end > *next {
            *next = batch_end;
        }
        self.cond.notify_all();
    }

    /// Unblocks every waiter after a worker has failed, so the pool can
    /// unwind instead of deadlocking on a batch that will never complete.
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
        self.cond.notify_all();
    }
}

/// Wraps any parallel processor so its `on_batch_complete` calls are
/// serialized to match the original order of records in the input
/// stream(s), at the cost of head-of-line blocking on the slowest
/// outstanding batch.
///
/// `process_record`/`process_record_batch` still run fully in parallel;
/// only the commit step (`on_batch_complete`) is ordered. Processors that
/// follow the common pattern of buffering per-batch output locally and
/// flushing it in `on_batch_complete` get correctly ordered output with no
/// other changes required.
///
/// This is purely a convenience: it does nothing but forward every method
/// to the wrapped processor and override `requires_ordering` to return
/// `true`. If you own the processor type, it's equivalent (and one less
/// layer) to override `requires_ordering` directly in your own impl:
///
/// ```ignore
/// impl<Rf: Record> ParallelProcessor<Rf> for MyWriter {
///     // ...
///     fn requires_ordering(&self) -> bool {
///         true
///     }
/// }
/// ```
///
/// Reach for `Ordered` instead when you don't own the processor type -
/// e.g. wrapping a closure or a processor defined elsewhere:
///
/// ```ignore
/// let mut processor = Ordered(MyWriter::new(...));
/// reader.process_parallel(&mut processor, num_threads)?;
/// ```
#[derive(Clone)]
pub struct Ordered<P>(pub P);

impl<Rf: Record, P: ParallelProcessor<Rf>> ParallelProcessor<Rf> for Ordered<P> {
    fn process_record_batch(&mut self, records: impl Iterator<Item = Rf>) -> Result<()> {
        self.0.process_record_batch(records)
    }
    fn process_record(&mut self, record: Rf) -> Result<()> {
        self.0.process_record(record)
    }
    fn on_batch_complete(&mut self) -> Result<()> {
        self.0.on_batch_complete()
    }
    fn on_thread_complete(&mut self) -> Result<()> {
        self.0.on_thread_complete()
    }
    fn set_thread_id(&mut self, thread_id: usize) {
        self.0.set_thread_id(thread_id);
    }
    fn get_thread_id(&self) -> usize {
        self.0.get_thread_id()
    }
    fn requires_ordering(&self) -> bool {
        true
    }
}

impl<Rf: Record, P: PairedParallelProcessor<Rf>> PairedParallelProcessor<Rf> for Ordered<P> {
    fn process_record_pair_batch(
        &mut self,
        record_pairs: impl Iterator<Item = (Rf, Rf)>,
    ) -> Result<()> {
        self.0.process_record_pair_batch(record_pairs)
    }
    fn process_record_pair(&mut self, record1: Rf, record2: Rf) -> Result<()> {
        self.0.process_record_pair(record1, record2)
    }
    fn on_batch_complete(&mut self) -> Result<()> {
        self.0.on_batch_complete()
    }
    fn on_thread_complete(&mut self) -> Result<()> {
        self.0.on_thread_complete()
    }
    fn set_thread_id(&mut self, thread_id: usize) {
        self.0.set_thread_id(thread_id);
    }
    fn get_thread_id(&self) -> usize {
        self.0.get_thread_id()
    }
    fn requires_ordering(&self) -> bool {
        true
    }
}

impl<Rf: Record, P: MultiParallelProcessor<Rf>> MultiParallelProcessor<Rf> for Ordered<P> {
    fn process_multi_record_batch(
        &mut self,
        multi_records: impl Iterator<Item = SmallVec<[Rf; MAX_ARITY]>>,
    ) -> Result<()> {
        self.0.process_multi_record_batch(multi_records)
    }
    fn process_multi_record(&mut self, records: &[Rf]) -> Result<()> {
        self.0.process_multi_record(records)
    }
    fn on_batch_complete(&mut self) -> Result<()> {
        self.0.on_batch_complete()
    }
    fn on_thread_complete(&mut self) -> Result<()> {
        self.0.on_thread_complete()
    }
    fn set_thread_id(&mut self, thread_id: usize) {
        self.0.set_thread_id(thread_id);
    }
    fn get_thread_id(&self) -> usize {
        self.0.get_thread_id()
    }
    fn requires_ordering(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use parking_lot::Mutex;

    use super::Ordered;
    use crate::fastq;
    use crate::parallel::{ParallelProcessor, ParallelReader, ProcessError};
    use crate::Record;

    fn make_fastq(n: usize) -> Vec<u8> {
        (0..n)
            .flat_map(|i| format!("@seq{i}\nACGT\n+\nIIII\n").into_bytes())
            .collect()
    }

    fn record_index<Rf: Record>(record: &Rf) -> usize {
        record
            .id_str()
            .strip_prefix("seq")
            .unwrap()
            .parse()
            .unwrap()
    }

    /// Buffers the ids of records seen in the current batch, then flushes
    /// them to a shared, order-agnostic sink in `on_batch_complete` - the
    /// same "local buffer, flush on batch complete" pattern used by
    /// ordering-sensitive processors like a shared-writer flush.
    #[derive(Clone, Default)]
    struct RecordingProcessor {
        local_ids: Vec<usize>,
        emitted: Arc<Mutex<Vec<usize>>>,
    }

    impl<Rf: Record> ParallelProcessor<Rf> for RecordingProcessor {
        fn process_record(&mut self, record: Rf) -> Result<(), ProcessError> {
            let idx = record_index(&record);
            // Bias early-stream records to be slower to process than later
            // ones, to actively encourage out-of-order batch completion in
            // the absence of the order gate.
            if idx < 100 {
                thread::sleep(Duration::from_micros(500));
            }
            self.local_ids.push(idx);
            Ok(())
        }

        fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
            self.emitted.lock().extend(self.local_ids.drain(..));
            Ok(())
        }
    }

    const N_RECORDS: usize = 400;
    const BATCH_SIZE: usize = 4;

    #[test]
    fn test_ordered_wrapper_preserves_stream_order() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let emitted = Arc::new(Mutex::new(Vec::new()));
        let mut processor = Ordered(RecordingProcessor {
            local_ids: Vec::new(),
            emitted: emitted.clone(),
        });

        reader.process_parallel(&mut processor, 8).unwrap();

        let expected: Vec<usize> = (0..N_RECORDS).collect();
        assert_eq!(*emitted.lock(), expected);
    }

    #[test]
    fn test_ordered_wrapper_is_opt_in() {
        // The inner processor's own record-processing behavior is identical
        // whether or not it's wrapped - `Ordered` only changes *when*
        // `on_batch_complete` is allowed to run, not what either method does.
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let emitted = Arc::new(Mutex::new(Vec::new()));
        let mut processor = RecordingProcessor {
            local_ids: Vec::new(),
            emitted: emitted.clone(),
        };

        reader.process_parallel(&mut processor, 8).unwrap();

        let mut sorted = emitted.lock().clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..N_RECORDS).collect::<Vec<_>>());
    }

    #[test]
    fn test_ordered_wrapper_respects_range() {
        // Batches entirely before the requested offset are skipped without
        // calling on_batch_complete; the gate must still fast-forward past
        // them so the first real batch's wait_turn doesn't hang forever.
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let emitted = Arc::new(Mutex::new(Vec::new()));
        let mut processor = Ordered(RecordingProcessor {
            local_ids: Vec::new(),
            emitted: emitted.clone(),
        });

        reader
            .process_parallel_range(&mut processor, 8, 137..229)
            .unwrap();

        let expected: Vec<usize> = (137..229).collect();
        assert_eq!(*emitted.lock(), expected);
    }
}
