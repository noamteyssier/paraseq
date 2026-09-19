use itertools::Itertools;

use crate::parallel::processor::GenericProcessor;
use crate::parallel::{pool::process_parallel_pool_range, ThreadPool};
use crate::{Error, Result};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A Sync version of GenericReader, i.e. for types with internal mutexes that can be shared between threads.
pub(crate) trait MTGenericReader: Send + Sync {
    type RecordSet: Send + 'static;
    type Error: Into<Error>;
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

pub(crate) fn process_sequential_generic_range<S: MTGenericReader, T>(
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

/// Fixed thread count, backed by a non-resizing [`ThreadPool`]. The pool's
/// fixed-spawn-set worker loop (see [`crate::parallel::pool`]) is the single
/// implementation behind every parallel entry point; a plain thread count
/// here just means a pool whose target never moves.
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
    let num_threads = if num_threads == 0 {
        num_cpus::get()
    } else {
        num_threads
    };
    if num_threads == 1 {
        return process_sequential_generic_range(reader, processor, offset, limit);
    }

    let pool = ThreadPool::new(num_threads);
    process_parallel_pool_range(reader, processor, &pool, offset, limit)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::fastq;
    use crate::parallel::{
        MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor, ParallelReader,
    };
    use crate::{Error, Record};

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
        fn process_record(&mut self, _record: Rf) -> Result<(), Error> {
            self.local_count += 1;
            Ok(())
        }

        fn on_batch_complete(&mut self) -> Result<(), Error> {
            self.global_count
                .fetch_add(self.local_count, Ordering::Relaxed);
            self.local_count = 0;
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct IndexCollectingProcessor {
        local_indices: Vec<u64>,
        global_indices: Arc<std::sync::Mutex<Vec<u64>>>,
    }

    impl<Rf: Record> ParallelProcessor<Rf> for IndexCollectingProcessor {
        fn process_record(&mut self, record: Rf) -> Result<(), Error> {
            self.local_indices.push(record.index());
            Ok(())
        }

        fn on_batch_complete(&mut self) -> Result<(), Error> {
            self.global_indices
                .lock()
                .unwrap()
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
            fn process_record(&mut self, record: Rf) -> Result<(), Error> {
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

        let mut indices = processor.global_indices.lock().unwrap().clone();
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

        let indices = processor.global_indices.lock().unwrap().clone();
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

        let mut indices = processor.global_indices.lock().unwrap().clone();
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
        fn process_record_pair(&mut self, _r1: Rf, _r2: Rf) -> Result<(), Error> {
            self.local_count += 1;
            Ok(())
        }

        fn on_batch_complete(&mut self) -> Result<(), Error> {
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
        fn process_multi_record(&mut self, _records: &[Rf]) -> Result<(), Error> {
            self.local_count += 1;
            Ok(())
        }

        fn on_batch_complete(&mut self) -> Result<(), Error> {
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
