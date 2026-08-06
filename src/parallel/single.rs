use itertools::Itertools;

use crate::parallel::ordered::OrderGate;
use crate::parallel::processor::GenericProcessor;
use crate::parallel::{error::Result, ProcessError};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

/// A Sync version of GenericReader, i.e. for types with internal mutexes that can be shared between threads.
pub(crate) trait MTGenericReader: Send + Sync {
    type RecordSet: Send + 'static;
    type Error: Into<ProcessError>;
    type RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet;

    /// Fills `record`, returning the `[start, end)` range this batch
    /// occupies in the underlying stream(s), or `None` at EOF.
    fn fill(
        &self,
        record: &mut Self::RecordSet,
    ) -> std::result::Result<Option<(usize, usize)>, Self::Error>;
    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>>;
    fn set_num_threads(&mut self, _num_threads: usize) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

/// Hands out non-overlapping `[start, end)` ranges from a monotonically
/// increasing position counter, for `MTGenericReader::fill` implementors
/// to claim their batch's position in.
pub(crate) struct BatchCounter(AtomicUsize);

impl BatchCounter {
    pub(crate) fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    /// Claims `size` more positions and returns the `[start, end)` range.
    pub(crate) fn claim(&self, size: usize) -> (usize, usize) {
        let start = self.0.fetch_add(size, Ordering::Relaxed);
        (start, start + size)
    }
}

pub(crate) fn process_parallel_generic<S: MTGenericReader, T>(
    reader: S,
    processor: &mut T,
    num_threads: usize,
) -> Result<()>
where
    T: for<'a> GenericProcessor<S::RefRecord<'a>>,
{
    process_parallel_generic_range(reader, processor, num_threads, 0, None)
}

fn process_sequential_generic_range<S: MTGenericReader, T>(
    reader: S,
    processor: &mut T,
    offset: usize,
    limit: Option<usize>,
) -> Result<()>
where
    T: for<'a> GenericProcessor<S::RefRecord<'a>>,
{
    let mut record_set = reader.new_record_set();
    let mut records_processed = 0; // Records actually processed

    while let Some((batch_start, batch_end)) = reader.fill(&mut record_set).map_err(Into::into)? {
        let batch_size = batch_end - batch_start;

        // Skip entire batch if still before offset
        if batch_end <= offset {
            continue;
        }

        // Check if we've hit the limit
        if let Some(lim) = limit {
            if records_processed >= lim {
                break;
            }
        }

        // Calculate slice of this batch to process
        let skip_in_batch = offset.saturating_sub(batch_start);
        let remaining_quota = limit.map(|lim| lim - records_processed);
        let take_count = match remaining_quota {
            Some(quota) => (batch_size - skip_in_batch).min(quota),
            None => batch_size - skip_in_batch,
        };

        // Process only the relevant slice
        let records = S::iter(&record_set)
            .skip(skip_in_batch)
            .take(take_count)
            .map(|r| r.map_err(Into::into));

        records.process_results(|records| processor.process_record_batch(records))??;

        records_processed += take_count;
        processor.on_batch_complete()?;
    }
    processor.on_thread_complete()?;
    Ok(())
}

pub(crate) fn process_parallel_generic_range<S: MTGenericReader, T>(
    reader: S,
    processor: &mut T,
    num_threads: usize,
    offset: usize,
    limit: Option<usize>,
) -> Result<()>
where
    T: for<'a> GenericProcessor<S::RefRecord<'a>>,
{
    // A pool whose target never moves is exactly a fixed worker count. There is
    // one implementation rather than two, so the resizable path cannot drift
    // from the fixed one.
    process_parallel_pool_range(
        reader,
        processor,
        &crate::parallel::ThreadPool::new(num_threads),
        offset,
        limit,
    )
}

/// As [`process_parallel_generic_range`], but the worker count may change while
/// the run is in flight. See [`crate::parallel::ThreadPool`].
pub(crate) fn process_parallel_pool_range<S: MTGenericReader, T>(
    mut reader: S,
    processor: &mut T,
    pool: &crate::parallel::ThreadPool,
    offset: usize,
    limit: Option<usize>,
) -> Result<()>
where
    T: for<'a> GenericProcessor<S::RefRecord<'a>>,
{
    let mut num_threads = pool.share_target();
    if num_threads == 0 {
        num_threads = num_cpus::get();
    }
    // Only when the pool can never grow. A pool that *starts* at one worker but
    // may be resized has to take the parallel path anyway, or it could never
    // honour a later `set_threads` -- there is no pool in the sequential path.
    if num_threads == 1 && pool.share_max() == 1 {
        return process_sequential_generic_range(reader, processor, offset, limit);
    }

    reader.set_num_threads(num_threads).map_err(Into::into)?;

    let records_processed = Arc::new(AtomicUsize::default());
    let order_gate = Arc::new(OrderGate::new());
    let ordered = processor.requires_ordering();

    // Workers spawned after the initial wave are in no handle list, so their
    // errors are collected here rather than by `join`. `thread::scope` still
    // joins every worker before its closure returns, so reading this afterwards
    // cannot race.
    let first_error: Mutex<Option<ProcessError>> = Mutex::new(None);
    let next_id = AtomicUsize::new(0);
    // Set as soon as any worker sees the input end.
    //
    // Without it, a worker leaving at EOF drops `live` below `target`, every
    // remaining worker reads that as "the pool is short", and each spawns a
    // replacement that immediately hits EOF and does the same. Measured before
    // this existed: 3.1 million workers spawned for a 32-thread run.
    let finished = AtomicBool::new(false);
    // New workers are cloned from this, never from a running worker.
    //
    // `Clone` on a processor means "give me a fresh worker" -- the fixed path
    // only ever clones the caller's untouched instance. Cloning a worker
    // mid-flight instead copies whatever it has accumulated, and any processor
    // keeping per-thread tallies then double-counts them when both flush in
    // `on_thread_complete`. That turned an 8 million record file into 791
    // billion records.
    let template: Mutex<T> = Mutex::new(processor.clone());

    thread::scope(|scope| -> Result<()> {
        let reader = &reader;
        let ctx = WorkerCtx {
            reader,
            pool,
            next_id: &next_id,
            first_error: &first_error,
            finished: &finished,
            template: &template,
            order_gate: &order_gate,
            ordered,
            offset,
            limit,
        };

        // At least one worker always starts: `share_target` floors at one and a
        // share's live count starts at zero, so the first claim cannot fail.
        // That matters more than it looks -- a share that spawned no workers
        // would return immediately and silently skip its whole input, and no
        // later growth could rescue it, because only a running worker spawns
        // another.
        for _ in 0..num_threads {
            if !pool.try_claim_slot() {
                break;
            }
            spawn_worker(
                scope,
                &ctx,
                processor.clone(),
                reader.new_record_set(),
                records_processed.clone(),
            );
        }
        Ok(())
    })?;

    match first_error.into_inner().unwrap_or(None) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Everything a worker needs that is identical for every worker.
///
/// Grouped so that spawning one takes three arguments rather than a dozen, and
/// so a worker can hand the same context to a successor.
struct WorkerCtx<'env, S, T> {
    reader: &'env S,
    pool: &'env crate::parallel::ThreadPool,
    next_id: &'env AtomicUsize,
    first_error: &'env Mutex<Option<ProcessError>>,
    finished: &'env AtomicBool,
    template: &'env Mutex<T>,
    order_gate: &'env Arc<OrderGate>,
    ordered: bool,
    offset: usize,
    limit: Option<usize>,
}

impl<S, T> Clone for WorkerCtx<'_, S, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<S, T> Copy for WorkerCtx<'_, S, T> {}

/// Run one worker, and let it spawn successors when the pool grows.
///
/// Spawning from inside a worker rather than from a dedicated coordinator puts
/// the cost where it is already amortised: a worker checks the pool once per
/// *batch*, between finishing one and asking for the next. No coordinator
/// thread, and no polling interval to tune.
fn spawn_worker<'scope, 'env, S, T>(
    scope: &'scope thread::Scope<'scope, 'env>,
    ctx: &WorkerCtx<'env, S, T>,
    mut worker_processor: T,
    mut record_set: S::RecordSet,
    records_processed: Arc<AtomicUsize>,
) where
    S: MTGenericReader + Sync + 'env,
    T: for<'a> GenericProcessor<S::RefRecord<'a>> + Send + 'env,
{
    let ctx = *ctx;
    let thread_id = ctx.next_id.fetch_add(1, Ordering::Relaxed);
    scope.spawn(move || {
        let mut retired = false;

        // As in the fixed path: run the body in a closure so any error can
        // poison the order gate before propagating, rather than deadlocking
        // workers waiting on a batch that will never complete.
        let result: Result<()> = (|| {
            worker_processor.set_thread_id(thread_id);

            loop {
                if let Some(lim) = ctx.limit {
                    if records_processed.load(Ordering::Relaxed) >= lim {
                        ctx.finished.store(true, Ordering::Relaxed);
                        break;
                    }
                }

                let Some((batch_start, batch_end)) =
                    ctx.reader.fill(&mut record_set).map_err(Into::into)?
                else {
                    ctx.finished.store(true, Ordering::Relaxed);
                    break; // EOF
                };
                let batch_size = batch_end - batch_start;
                let range_end = ctx.limit.map(|lim| ctx.offset + lim).unwrap_or(usize::MAX);

                if batch_end <= ctx.offset {
                    if ctx.ordered {
                        ctx.order_gate.advance(batch_end);
                    }
                    continue;
                }
                if batch_start >= range_end {
                    ctx.finished.store(true, Ordering::Relaxed);
                    break;
                }

                let skip_in_batch = ctx.offset.saturating_sub(batch_start);
                let take_count =
                    (batch_size - skip_in_batch).min(range_end - batch_start - skip_in_batch);

                let records = S::iter(&record_set)
                    .skip(skip_in_batch)
                    .take(take_count)
                    .map(|r| r.map_err(Into::into));

                records
                    .process_results(|records| worker_processor.process_record_batch(records))??;

                records_processed.fetch_add(take_count, Ordering::Relaxed);

                if ctx.ordered {
                    ctx.order_gate.wait_turn(batch_start);
                }
                worker_processor.on_batch_complete()?;
                if ctx.ordered {
                    ctx.order_gate.advance(batch_end);
                }

                // Resize *here*, after the order gate has been advanced past
                // this batch. Retiring while the gate still expects this worker
                // to advance it would stall every other worker behind a turn
                // that never comes.
                //
                // The whole steady-state cost of a resizable pool: two relaxed
                // loads, taking neither branch unless the target has moved.
                if ctx.pool.try_release_slot() {
                    retired = true;
                    break;
                }
                while !ctx.finished.load(Ordering::Relaxed) && ctx.pool.try_claim_slot() {
                    let fresh = ctx.template.lock().unwrap().clone();
                    // Allocated on this thread rather than the new worker's, so
                    // the allocation pattern matches the fixed path exactly.
                    // Still only when a worker is really created, so nothing is
                    // pre-allocated for a worker that never exists.
                    let fresh_set = ctx.reader.new_record_set();
                    spawn_worker(scope, &ctx, fresh, fresh_set, records_processed.clone());
                }
            }
            // Retiring workers flush too: `on_thread_complete` is where a
            // processor commits its per-thread state, and a worker that leaves
            // mid-run has just as much to commit as one that reaches EOF.
            worker_processor.on_thread_complete()?;
            Ok(())
        })();

        if !retired {
            ctx.pool.release_slot();
        }
        if let Err(e) = result {
            if ctx.ordered {
                ctx.order_gate.poison();
            }
            let mut slot = ctx.first_error.lock().unwrap();
            if slot.is_none() {
                *slot = Some(e);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::fastq;
    use crate::parallel::{
        MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor, ParallelReader,
        ProcessError,
    };
    use crate::Record;

    fn make_fastq(n: usize) -> Vec<u8> {
        (0..n)
            .flat_map(|i| format!("@seq{i}\nACGT\n+\nIIII\n").into_bytes())
            .collect()
    }

    #[derive(Clone, Default)]
    struct CountingProcessor {
        local_count: usize,
        global_count: Arc<AtomicUsize>,
    }

    impl CountingProcessor {
        fn count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }

    impl<Rf: Record> ParallelProcessor<Rf> for CountingProcessor {
        fn process_record(&mut self, _record: Rf) -> Result<(), ProcessError> {
            self.local_count += 1;
            Ok(())
        }

        fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
            self.global_count
                .fetch_add(self.local_count, Ordering::Relaxed);
            self.local_count = 0;
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct IndexCollectingProcessor {
        local_indices: Vec<u64>,
        global_indices: Arc<parking_lot::Mutex<Vec<u64>>>,
    }

    impl<Rf: Record> ParallelProcessor<Rf> for IndexCollectingProcessor {
        fn process_record(&mut self, record: Rf) -> Result<(), ProcessError> {
            self.local_indices.push(record.index());
            Ok(())
        }

        fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
            self.global_indices
                .lock()
                .extend(self.local_indices.drain(..));
            Ok(())
        }
    }

    const N_RECORDS: usize = 500;
    const BATCH_SIZE: usize = 10;
    const LIMIT: usize = 50;

    fn make_limited_reader(data: Vec<u8>, limit: usize) -> fastq::Reader<Cursor<Vec<u8>>> {
        let mut reader = fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
        reader.set_record_limit(limit);
        reader
    }

    #[test]
    fn test_record_limit_sequential() {
        let reader = make_limited_reader(make_fastq(N_RECORDS), LIMIT);
        let mut processor = CountingProcessor::default();

        reader.process_parallel(&mut processor, 1).unwrap();

        assert_eq!(processor.count(), LIMIT);
    }

    #[test]
    fn test_record_limit_parallel() {
        let reader = make_limited_reader(make_fastq(N_RECORDS), LIMIT);
        let mut processor = CountingProcessor::default();

        reader.process_parallel(&mut processor, 4).unwrap();

        assert_eq!(processor.count(), LIMIT);
    }

    #[test]
    fn test_record_limit_non_multiple_of_batch() {
        // 45 is not a multiple of BATCH_SIZE (10), so the last batch is truncated.
        let reader = make_limited_reader(make_fastq(N_RECORDS), 45);
        let mut processor = CountingProcessor::default();

        reader.process_parallel(&mut processor, 4).unwrap();

        assert_eq!(processor.count(), 45);
    }

    #[test]
    fn test_no_limit_processes_all_sequential() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel(&mut processor, 1).unwrap();

        assert_eq!(processor.count(), N_RECORDS);
    }

    #[test]
    fn test_no_limit_processes_all_parallel() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel(&mut processor, 4).unwrap();

        assert_eq!(processor.count(), N_RECORDS);
    }

    #[test]
    fn test_record_limit_larger_than_file() {
        // Limit larger than file: process all available records.
        let reader = make_limited_reader(make_fastq(N_RECORDS), N_RECORDS * 2);
        let mut processor = CountingProcessor::default();

        reader.process_parallel(&mut processor, 4).unwrap();

        assert_eq!(processor.count(), N_RECORDS);
    }

    #[test]
    fn test_range_basic_sequential() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 1, 10..20)
            .unwrap();

        assert_eq!(processor.count(), 10);
    }

    #[test]
    fn test_range_basic_parallel() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 4, 10..20)
            .unwrap();

        assert_eq!(processor.count(), 10);
    }

    #[test]
    fn test_range_from_start() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 4, 0..50)
            .unwrap();

        assert_eq!(processor.count(), 50);
    }

    #[test]
    fn test_range_to_end() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 4, 450..)
            .unwrap();

        assert_eq!(processor.count(), 50);
    }

    #[test]
    fn test_range_beyond_eof() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 4, 400..1000)
            .unwrap();

        assert_eq!(processor.count(), 100);
    }

    #[test]
    fn test_range_empty() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 4, 100..100)
            .unwrap();

        assert_eq!(processor.count(), 0);
    }

    #[test]
    fn test_range_non_batch_aligned() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 4, 17..83)
            .unwrap();

        assert_eq!(processor.count(), 66);
    }

    // Regression test for a race between claiming a batch's stream position
    // (`records_seen`) and the reader's own internal lock around `fill`.
    // The two used to happen as separate steps, so a thread could be
    // preempted between them and let a later-read chunk claim an earlier
    // position (or vice versa), corrupting which records offset/limit
    // slicing believes it's looking at. Small batches, many threads, and an
    // uneven per-record delay (to perturb scheduling) reproduce it reliably
    // without the fix; this asserts the exact record set survives regardless.
    #[test]
    fn test_range_exact_records_under_contention() {
        use std::sync::Mutex;
        use std::thread;
        use std::time::Duration;

        #[derive(Clone, Default)]
        struct IdCollector {
            seen: Arc<Mutex<Vec<usize>>>,
        }

        impl<Rf: Record> ParallelProcessor<Rf> for IdCollector {
            fn process_record(&mut self, record: Rf) -> Result<(), ProcessError> {
                let idx: usize = record
                    .id_str()
                    .strip_prefix("seq")
                    .unwrap()
                    .parse()
                    .unwrap();
                // Bias early records to be slower, widening the window in
                // which the fill/claim race (if reintroduced) could hit.
                if idx < 100 {
                    thread::sleep(Duration::from_micros(200));
                }
                self.seen.lock().unwrap().push(idx);
                Ok(())
            }
        }

        for attempt in 0..20 {
            let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(400)), 4).unwrap();
            let mut processor = IdCollector::default();

            reader
                .process_parallel_range(&mut processor, 16, 137..229)
                .unwrap();

            let mut got = processor.seen.lock().unwrap().clone();
            got.sort_unstable();
            let expected: Vec<usize> = (137..229).collect();
            assert_eq!(got, expected, "mismatch on attempt {attempt}");
        }
    }

    #[test]
    fn test_range_single_batch() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 4, 15..22)
            .unwrap();

        assert_eq!(processor.count(), 7);
    }

    #[test]
    fn test_range_inclusive() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 4, 10..=19)
            .unwrap();

        assert_eq!(processor.count(), 10);
    }

    #[test]
    fn test_index_reflects_true_file_position_under_range() {
        // A range only filters which records reach the processor; the
        // `index()` of a delivered record must still be its true position
        // in the file, not an offset relative to the range.
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = IndexCollectingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 4, 10..20)
            .unwrap();

        let mut indices = processor.global_indices.lock().clone();
        indices.sort_unstable();
        assert_eq!(indices, (10..20u64).collect::<Vec<_>>());
    }

    #[test]
    fn test_index_reflects_true_file_position_under_range_sequential() {
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = IndexCollectingProcessor::default();

        reader
            .process_parallel_range(&mut processor, 1, 17..83)
            .unwrap();

        let indices = processor.global_indices.lock().clone();
        assert_eq!(indices, (17..83u64).collect::<Vec<_>>());
    }

    #[test]
    fn test_index_complete_and_unique_multi_threaded() {
        // Batches are claimed atomically by threads, so delivery order isn't
        // guaranteed, but every index in the file must be assigned to
        // exactly one record.
        let reader =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = IndexCollectingProcessor::default();

        reader.process_parallel(&mut processor, 4).unwrap();

        let mut indices = processor.global_indices.lock().clone();
        indices.sort_unstable();
        assert_eq!(indices, (0..N_RECORDS as u64).collect::<Vec<_>>());
    }

    // Paired range tests
    #[derive(Clone, Default)]
    struct PairedCountingProcessor {
        local_count: usize,
        global_count: Arc<AtomicUsize>,
    }

    impl PairedCountingProcessor {
        fn count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }

    impl<Rf: Record> PairedParallelProcessor<Rf> for PairedCountingProcessor {
        fn process_record_pair(&mut self, _r1: Rf, _r2: Rf) -> Result<(), ProcessError> {
            self.local_count += 1;
            Ok(())
        }

        fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
            self.global_count
                .fetch_add(self.local_count, Ordering::Relaxed);
            self.local_count = 0;
            Ok(())
        }
    }

    #[test]
    fn test_range_paired_basic() {
        let r1 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = PairedCountingProcessor::default();

        r1.process_parallel_paired_range(r2, &mut processor, 4, 10..30)
            .unwrap();

        assert_eq!(processor.count(), 20);
    }

    #[test]
    fn test_range_paired_sequential() {
        let r1 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = PairedCountingProcessor::default();

        r1.process_parallel_paired_range(r2, &mut processor, 1, 5..15)
            .unwrap();

        assert_eq!(processor.count(), 10);
    }

    #[test]
    fn test_range_interleaved_basic() {
        let data = make_fastq(N_RECORDS);
        let reader = fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
        let mut processor = PairedCountingProcessor::default();

        reader
            .process_parallel_interleaved_range(&mut processor, 4, 10..30)
            .unwrap();

        assert_eq!(processor.count(), 20); // 20 pairs (40 file records)
    }

    #[test]
    fn test_range_interleaved_from_start() {
        let data = make_fastq(N_RECORDS);
        let reader = fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
        let mut processor = PairedCountingProcessor::default();

        reader
            .process_parallel_interleaved_range(&mut processor, 4, 0..20)
            .unwrap();

        assert_eq!(processor.count(), 20); // 20 pairs (40 file records)
    }

    // Multi range tests
    #[derive(Clone, Default)]
    struct MultiCountingProcessor {
        local_count: usize,
        global_count: Arc<AtomicUsize>,
    }

    impl MultiCountingProcessor {
        fn count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }

    impl<Rf: Record> MultiParallelProcessor<Rf> for MultiCountingProcessor {
        fn process_multi_record(&mut self, _records: &[Rf]) -> Result<(), ProcessError> {
            self.local_count += 1;
            Ok(())
        }

        fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
            self.global_count
                .fetch_add(self.local_count, Ordering::Relaxed);
            self.local_count = 0;
            Ok(())
        }
    }

    #[test]
    fn test_range_multi_basic() {
        let r1 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r3 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = MultiCountingProcessor::default();

        r1.process_parallel_multi_range(vec![r2, r3], &mut processor, 4, 10..30)
            .unwrap();

        assert_eq!(processor.count(), 20);
    }

    #[test]
    fn test_range_multi_interleaved_basic() {
        // Process 10..30 = 20 record-groups (100 file records with arity=5)
        let data = make_fastq(N_RECORDS);
        let reader = fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
        let mut processor = MultiCountingProcessor::default();

        reader
            .process_parallel_multi_interleaved_range(5, &mut processor, 4, 10..30)
            .unwrap();

        assert_eq!(processor.count(), 20); // 20 record-groups
    }

    #[test]
    fn test_range_multi_interleaved_from_start() {
        // Process 0..20 = 20 record-groups (40 file records with arity=2)
        let data = make_fastq(N_RECORDS);
        let reader = fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
        let mut processor = MultiCountingProcessor::default();

        reader
            .process_parallel_multi_interleaved_range(2, &mut processor, 4, 0..20)
            .unwrap();

        assert_eq!(processor.count(), 20); // 20 record-groups
    }

    // Test that range semantics are consistent across modes
    #[test]
    fn test_range_semantic_consistency() {
        // All of these should process the same NUMBER of semantic units (50)
        // even though they read different numbers of file records

        // Single: 50 records from file
        let r1 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p1 = CountingProcessor::default();
        r1.process_parallel_range(&mut p1, 4, 0..50).unwrap();
        assert_eq!(p1.count(), 50, "single-ended should process 50 records");

        // Paired: 50 pairs (50 records from each file = 100 total file records)
        let r1 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p2 = PairedCountingProcessor::default();
        r1.process_parallel_paired_range(r2, &mut p2, 4, 0..50)
            .unwrap();
        assert_eq!(p2.count(), 50, "paired should process 50 pairs");

        // Interleaved: 50 pairs (100 file records)
        let r1 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p3 = PairedCountingProcessor::default();
        r1.process_parallel_interleaved_range(&mut p3, 4, 0..50)
            .unwrap();
        assert_eq!(p3.count(), 50, "interleaved should process 50 pairs");

        // Multi (arity 2): 50 record-groups (50 records from each of 2 files = 100 total)
        let r1 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p4 = MultiCountingProcessor::default();
        r1.process_parallel_multi_range(vec![r2], &mut p4, 4, 0..50)
            .unwrap();
        assert_eq!(p4.count(), 50, "multi should process 50 record-groups");

        // Multi-interleaved (arity 5): 50 record-groups (250 file records)
        // BATCH_SIZE=10 divides evenly by arity=5
        let r1 =
            fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p5 = MultiCountingProcessor::default();
        r1.process_parallel_multi_interleaved_range(5, &mut p5, 4, 0..50)
            .unwrap();
        assert_eq!(
            p5.count(),
            50,
            "multi-interleaved should process 50 record-groups"
        );
    }
}

#[cfg(test)]
mod pool_tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::fastq;
    use crate::parallel::{ParallelProcessor, ParallelReader, ProcessError, ThreadPool};
    use crate::Record;

    fn make_fastq(n: usize) -> Vec<u8> {
        (0..n)
            .flat_map(|i| format!("@seq{i}\nACGT\n+\nIIII\n").into_bytes())
            .collect()
    }

    /// Deliberately keeps per-thread state and only publishes it in
    /// `on_thread_complete`, exactly as a real processor does. That is what
    /// makes it sensitive to *where* a new worker's clone comes from.
    #[derive(Clone, Default)]
    struct TallyProcessor {
        total: Arc<AtomicUsize>,
        threads_completed: Arc<AtomicUsize>,
        local: usize,
    }

    impl<Rf: Record> ParallelProcessor<Rf> for TallyProcessor {
        fn process_record(&mut self, _record: Rf) -> Result<(), ProcessError> {
            self.local += 1;
            Ok(())
        }
        fn on_thread_complete(&mut self) -> Result<(), ProcessError> {
            self.total.fetch_add(self.local, Ordering::Relaxed);
            self.threads_completed.fetch_add(1, Ordering::Relaxed);
            self.local = 0;
            Ok(())
        }
    }

    /// A pool that never resizes must behave exactly like a fixed count.
    #[test]
    fn a_fixed_pool_matches_a_fixed_thread_count() {
        const N: usize = 5_000;
        for threads in [1usize, 2, 8] {
            let mut proc = TallyProcessor::default();
            let reader = fastq::Reader::new(Cursor::new(make_fastq(N)));
            reader
                .process_parallel_pool(&mut proc, &ThreadPool::new(threads))
                .unwrap();
            assert_eq!(proc.total.load(Ordering::Relaxed), N, "threads {threads}");
        }
    }

    /// Regression: a worker leaving at EOF drops `live` below `target`, and
    /// every remaining worker used to read that as "the pool is short" and
    /// spawn a replacement, which hit EOF and did the same. That produced 3.1
    /// million workers for a 32-thread run over an 8 M record file.
    ///
    /// The record count catches it; `threads_completed` catches it loudly.
    #[test]
    fn workers_leaving_at_eof_do_not_spawn_replacements() {
        const N: usize = 20_000;
        let mut proc = TallyProcessor::default();
        let reader = fastq::Reader::new(Cursor::new(make_fastq(N)));
        reader
            .process_parallel_pool(&mut proc, &ThreadPool::new(8))
            .unwrap();

        assert_eq!(proc.total.load(Ordering::Relaxed), N);
        assert!(
            proc.threads_completed.load(Ordering::Relaxed) <= 8,
            "{} workers ran for an 8-worker pool: EOF is being mistaken for a \
             shortfall and workers are respawning each other",
            proc.threads_completed.load(Ordering::Relaxed)
        );
    }

    /// Regression: new workers must be cloned from the caller's pristine
    /// processor, never from a running one.
    ///
    /// `Clone` on a processor means "give me a fresh worker". Cloning a worker
    /// mid-flight copies its accumulated `local`, and both copies then publish
    /// it in `on_thread_complete` -- turning 8 M records into 791 billion.
    #[test]
    fn grown_workers_start_from_a_pristine_processor() {
        const N: usize = 200_000;
        let pool = ThreadPool::with_max(1, 8);
        let mut proc = TallyProcessor::default();

        let p2 = pool.clone();
        let grower = std::thread::spawn(move || {
            for n in 2..=8 {
                std::thread::sleep(std::time::Duration::from_micros(200));
                p2.set_threads(n);
            }
        });

        let reader = fastq::Reader::new(Cursor::new(make_fastq(N)));
        reader.process_parallel_pool(&mut proc, &pool).unwrap();
        grower.join().unwrap();

        assert_eq!(
            proc.total.load(Ordering::Relaxed),
            N,
            "records were counted more than once: a growing worker inherited \
             another worker's partial tally"
        );
    }

    /// Growth has to be reachable from a single worker, which means the
    /// `num_threads == 1` sequential short-circuit must look at whether the
    /// pool *can* grow, not at where it starts.
    #[test]
    fn a_pool_starting_at_one_can_still_grow() {
        const N: usize = 200_000;
        let pool = ThreadPool::with_max(1, 4);
        let mut proc = TallyProcessor::default();

        let p2 = pool.clone();
        let grower = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_micros(500));
            p2.set_threads(4);
        });

        let reader = fastq::Reader::new(Cursor::new(make_fastq(N)));
        reader.process_parallel_pool(&mut proc, &pool).unwrap();
        grower.join().unwrap();

        assert_eq!(proc.total.load(Ordering::Relaxed), N);
    }

    /// Shrinking must never retire the last worker.
    ///
    /// The assertion is that this test *finishes*. If the pool could empty
    /// itself there would be no worker left to reach EOF, `thread::scope` would
    /// wait on threads that no longer exist to be created, and the test would
    /// hang rather than fail. Sampling `live()` instead would race the end of
    /// the run, where zero is the correct answer.
    #[test]
    fn shrinking_always_leaves_one_worker() {
        const N: usize = 400_000;
        let pool = ThreadPool::with_max(8, 8);
        let mut proc = TallyProcessor::default();

        let p2 = pool.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s2 = stop.clone();
        let shrinker = std::thread::spawn(move || {
            for n in (1..=8).rev() {
                if s2.load(Ordering::Relaxed) {
                    break;
                }
                p2.set_threads(n);
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
        });

        let reader = fastq::Reader::new(Cursor::new(make_fastq(N)));
        reader.process_parallel_pool(&mut proc, &pool).unwrap();
        stop.store(true, Ordering::Relaxed);
        shrinker.join().unwrap();

        assert_eq!(proc.total.load(Ordering::Relaxed), N);
    }

    /// The property that has to hold under arbitrary resizing: every record is
    /// processed exactly once, however many workers come and go.
    #[test]
    fn thread_churn_processes_every_record_exactly_once() {
        const N: usize = 200_000;
        let pool = ThreadPool::with_max(4, 16);
        let mut proc = TallyProcessor::default();

        let p2 = pool.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s2 = stop.clone();
        let churner = std::thread::spawn(move || {
            let mut n = 1;
            while !s2.load(Ordering::Relaxed) {
                n = if n >= 16 { 1 } else { n + 3 };
                p2.set_threads(n);
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
        });

        let reader = fastq::Reader::new(Cursor::new(make_fastq(N)));
        reader.process_parallel_pool(&mut proc, &pool).unwrap();
        stop.store(true, Ordering::Relaxed);
        churner.join().unwrap();

        assert_eq!(proc.total.load(Ordering::Relaxed), N);
    }

    /// A supervisor outside the run needs the aggregate live count.
    ///
    /// `live()` reports one share, and the pool a caller holds is the *parent*,
    /// whose own share never runs anything once a `Collection` splits it — so the
    /// obvious reading is zero for the whole run. An external scheduler normalising
    /// work against "threads running" then divides by nothing and concludes the
    /// consumer is never busy.
    #[test]
    fn total_live_counts_workers_across_every_share() {
        use crate::fastx::{Collection, CollectionType};

        const N: usize = 200_000;
        let pool = ThreadPool::with_max(8, 8);
        let observer = pool.clone();
        let seen = Arc::new(AtomicUsize::new(0));
        let s2 = Arc::clone(&seen);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let st2 = Arc::clone(&stop);
        let watcher = std::thread::spawn(move || {
            while !st2.load(Ordering::Relaxed) {
                s2.fetch_max(observer.total_live(), Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
        });

        let mut proc = TallyProcessor::default();
        let inner: Vec<_> = (0..4)
            .map(|_| crate::fastx::Reader::new(Cursor::new(make_fastq(N))).unwrap())
            .collect();
        Collection::new(inner, CollectionType::Single)
            .unwrap()
            .process_parallel_pool(&mut proc, &pool, None)
            .unwrap();
        stop.store(true, Ordering::Relaxed);
        watcher.join().unwrap();

        assert_eq!(proc.total.load(Ordering::Relaxed), N * 4);
        assert!(
            seen.load(Ordering::Relaxed) > 1,
            "peak total_live was {}, so shares are invisible to the parent",
            seen.load(Ordering::Relaxed)
        );
        // And it unwinds: nothing is left counted after the run.
        assert_eq!(pool.total_live(), 0, "live count leaked after the run");
    }

    /// A `Collection` splits its pool across the readers running at once.
    ///
    /// The regression this guards: handing every reader the *same* pool lets
    /// the first claim every slot, so the rest spawn nothing. Since only a
    /// running worker spawns another, those readers never start and their input
    /// is silently skipped -- the counts come back short, with no error.
    #[test]
    fn a_collection_processes_every_reader_from_a_shared_pool() {
        use crate::fastx::{Collection, CollectionType};

        const N: usize = 5_000;
        for readers in [1usize, 2, 4, 8] {
            let mut proc = TallyProcessor::default();
            let inner: Vec<_> = (0..readers)
                .map(|_| crate::fastx::Reader::new(Cursor::new(make_fastq(N))).unwrap())
                .collect();
            let collection = Collection::new(inner, CollectionType::Single).unwrap();
            // Fewer threads than readers, so the split rounds down toward zero
            // and every share has to be floored at one.
            collection
                .process_parallel_pool(&mut proc, &ThreadPool::new(2), None)
                .unwrap();
            assert_eq!(
                proc.total.load(Ordering::Relaxed),
                N * readers,
                "{readers} readers: some reader was never started"
            );
        }
    }

    /// The same, for the grouped (paired) dispatch path.
    #[test]
    fn a_paired_collection_processes_every_group() {
        use crate::fastx::{Collection, CollectionType};

        const N: usize = 4_000;
        #[derive(Clone, Default)]
        struct PairTally {
            total: Arc<AtomicUsize>,
            local: usize,
        }
        impl<Rf: Record> crate::parallel::PairedParallelProcessor<Rf> for PairTally {
            fn process_record_pair(&mut self, _r1: Rf, _r2: Rf) -> Result<(), ProcessError> {
                self.local += 1;
                Ok(())
            }
            fn on_thread_complete(&mut self) -> Result<(), ProcessError> {
                self.total.fetch_add(self.local, Ordering::Relaxed);
                self.local = 0;
                Ok(())
            }
        }

        for pairs in [1usize, 2, 4] {
            let mut proc = PairTally::default();
            let inner: Vec<_> = (0..pairs * 2)
                .map(|_| crate::fastx::Reader::new(Cursor::new(make_fastq(N))).unwrap())
                .collect();
            let collection = Collection::new(inner, CollectionType::Paired).unwrap();
            collection
                .process_parallel_paired_pool(&mut proc, &ThreadPool::new(2), None)
                .unwrap();
            assert_eq!(
                proc.total.load(Ordering::Relaxed),
                N * pairs,
                "{pairs} pairs: some group was never started"
            );
        }
    }

    /// A share reads the same target as its parent, so one `set_threads`
    /// retargets every reader at once.
    #[test]
    fn shares_track_the_parent_target() {
        let pool = ThreadPool::with_max(16, 32);
        let a = pool.share(4);
        let b = pool.share(4);
        assert_eq!(a.share_target(), 4);
        assert_eq!(b.share_target(), 4);

        pool.set_threads(32);
        assert_eq!(a.share_target(), 8, "a share must see the parent resize");
        assert_eq!(b.share_target(), 8);

        // A share never rounds down to zero workers.
        let thin = pool.share(64);
        assert_eq!(thin.share_target(), 1);
        assert_eq!(thin.share_max(), 1);

        // Live counts are per share, not shared: claiming in one leaves the
        // other untouched.
        assert!(a.try_claim_slot());
        assert_eq!(a.live(), 1);
        assert_eq!(b.live(), 0);
    }

    /// A pool never exceeds the ceiling it was built with.
    #[test]
    fn the_maximum_is_respected() {
        let pool = ThreadPool::with_max(2, 4);
        pool.set_threads(999);
        assert_eq!(pool.threads(), 4);
        pool.set_threads(0);
        assert_eq!(
            pool.threads(),
            1,
            "a pool must always want at least one worker"
        );
    }
}
