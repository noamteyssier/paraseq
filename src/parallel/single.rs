use itertools::Itertools;
use parking_lot::Mutex;

use crate::fastx::GenericReader;
use crate::parallel::processor::GenericProcessor;
use crate::parallel::{error::Result, ProcessError};
use crate::Record;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use super::{
    multi::InterleavedMultiReader, multi::MultiReader, paired::InterleavedPairedReader,
    paired::PairedReader, MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor,
};

/// A Sync version of GenericReader, i.e. for types with internal mutexes that can be shared between threads.
pub(crate) trait MTGenericReader: Send + Sync {
    type RecordSet: Send + 'static;
    type Error: Into<ProcessError>;
    type RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet;
    fn fill(&self, record: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error>;
    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>>;
    fn n_records(record_set: &Self::RecordSet) -> usize;
    fn set_num_threads(&mut self, _num_threads: usize) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

/// Helper to convert RangeBounds to (start, limit)
fn range_to_offset_limit(range: impl RangeBounds<usize>) -> (usize, Option<usize>) {
    use std::ops::Bound;

    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n + 1,
        Bound::Unbounded => 0,
    };

    let limit = match range.end_bound() {
        Bound::Included(&n) => Some(n + 1 - start),
        Bound::Excluded(&n) => Some(n - start),
        Bound::Unbounded => None,
    };

    (start, limit)
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
    let mut records_seen = 0; // Total records encountered
    let mut records_processed = 0; // Records actually processed

    while reader.fill(&mut record_set).map_err(Into::into)? {
        let batch_size = S::n_records(&record_set);

        // Skip entire batch if still before offset
        if records_seen + batch_size <= offset {
            records_seen += batch_size;
            continue;
        }

        // Check if we've hit the limit
        if let Some(lim) = limit {
            if records_processed >= lim {
                break;
            }
        }

        // Calculate slice of this batch to process
        let skip_in_batch = offset.saturating_sub(records_seen);
        let remaining_quota = limit.map(|lim| lim - records_processed);
        let take_count = match remaining_quota {
            Some(quota) => (batch_size - skip_in_batch).min(quota),
            None => batch_size - skip_in_batch,
        };

        records_seen += batch_size;

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
    mut reader: S,
    processor: &mut T,
    mut num_threads: usize,
    offset: usize,
    limit: Option<usize>,
) -> Result<()>
where
    T: for<'a> GenericProcessor<S::RefRecord<'a>>,
{
    if num_threads == 0 {
        num_threads = num_cpus::get();
    }
    if num_threads == 1 {
        return process_sequential_generic_range(reader, processor, offset, limit);
    }

    reader.set_num_threads(num_threads).map_err(Into::into)?;

    let records_seen = Arc::new(AtomicUsize::default());
    let records_processed = Arc::new(AtomicUsize::default());

    thread::scope(|scope| -> Result<()> {
        let reader = &reader;

        let mut handles = Vec::new();
        for thread_id in 0..num_threads {
            let mut worker_processor = processor.clone();
            let mut record_set = reader.new_record_set();
            let records_seen = records_seen.clone();
            let records_processed = records_processed.clone();

            let handle = scope.spawn(move || {
                worker_processor.set_thread_id(thread_id);

                loop {
                    // Check limit before grabbing batch
                    if let Some(lim) = limit {
                        if records_processed.load(Ordering::Relaxed) >= lim {
                            break;
                        }
                    }

                    // Fill the batch
                    if !reader.fill(&mut record_set).map_err(Into::into)? {
                        break; // EOF
                    }

                    let batch_size = S::n_records(&record_set);

                    // Atomically claim our position in the stream
                    let batch_start = records_seen.fetch_add(batch_size, Ordering::SeqCst);
                    let batch_end = batch_start + batch_size;

                    // Determine overlap with target range [offset, offset+limit)
                    let range_end = limit.map(|lim| offset + lim).unwrap_or(usize::MAX);

                    if batch_end <= offset {
                        // Entire batch before offset - skip it
                        continue;
                    }

                    if batch_start >= range_end {
                        // Entire batch after limit - done
                        break;
                    }

                    // Calculate slice of this batch within range
                    let skip_in_batch = offset.saturating_sub(batch_start);
                    let take_count =
                        (batch_size - skip_in_batch).min(range_end - batch_start - skip_in_batch);

                    // Process the slice
                    let records = S::iter(&record_set)
                        .skip(skip_in_batch)
                        .take(take_count)
                        .map(|r| r.map_err(Into::into));

                    records.process_results(|records| {
                        worker_processor.process_record_batch(records)
                    })??;

                    records_processed.fetch_add(take_count, Ordering::Relaxed);
                    worker_processor.on_batch_complete()?;
                }
                worker_processor.on_thread_complete()?;
                Ok(())
            });

            handles.push(handle);
        }

        // Wait for workers
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => (),
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(ProcessError::JoinError),
            }
        }

        Ok(())
    })?;

    Ok(())
}

pub trait ParallelReader {
    type Rf<'a>: Record;

    fn process_parallel<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
    where
        T: for<'a> ParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_range<T>(
        self,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> ParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_paired<T>(
        self,
        r2: Self,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_paired_range<T>(
        self,
        r2: Self,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_interleaved<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_interleaved_range<T>(
        self,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_multi<T>(
        self,
        rest: Vec<Self>,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>,
        Self: Sized;

    fn process_parallel_multi_range<T>(
        self,
        rest: Vec<Self>,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>,
        Self: Sized;

    fn process_parallel_multi_interleaved<T>(
        self,
        arity: usize,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_multi_interleaved_range<T>(
        self,
        arity: usize,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>;
}

impl<S: GenericReader> ParallelReader for S
where
    for<'a> <S as GenericReader>::RefRecord<'a>: Record,
    ProcessError: From<S::Error>,
{
    type Rf<'a> = S::RefRecord<'a>;

    fn process_parallel<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
    where
        T: for<'a> ParallelProcessor<S::RefRecord<'a>>,
    {
        process_parallel_generic(SingleReader::new(self), processor, num_threads)
    }

    fn process_parallel_range<T>(
        self,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> ParallelProcessor<S::RefRecord<'a>>,
    {
        let (start, limit) = range_to_offset_limit(range);
        process_parallel_generic_range(
            SingleReader::new(self),
            processor,
            num_threads,
            start,
            limit,
        )
    }

    fn process_parallel_interleaved<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>,
    {
        process_parallel_generic(InterleavedPairedReader::new(self), processor, num_threads)
    }

    fn process_parallel_interleaved_range<T>(
        self,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>,
    {
        let (start, limit) = range_to_offset_limit(range);
        process_parallel_generic_range(
            InterleavedPairedReader::new(self),
            processor,
            num_threads,
            start,
            limit,
        )
    }

    fn process_parallel_paired<T>(
        self,
        r2: Self,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>,
    {
        process_parallel_generic(PairedReader::new(self, r2), processor, num_threads)
    }

    fn process_parallel_paired_range<T>(
        self,
        r2: Self,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>,
    {
        let (start, limit) = range_to_offset_limit(range);
        process_parallel_generic_range(
            PairedReader::new(self, r2),
            processor,
            num_threads,
            start,
            limit,
        )
    }

    fn process_parallel_multi<T>(
        self,
        mut rest: Vec<Self>,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>,
        Self: Sized,
    {
        rest.insert(0, self);
        process_parallel_generic(MultiReader::new(rest), processor, num_threads)
    }

    fn process_parallel_multi_range<T>(
        self,
        mut rest: Vec<Self>,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>,
        Self: Sized,
    {
        rest.insert(0, self);
        let (start, limit) = range_to_offset_limit(range);
        process_parallel_generic_range(
            MultiReader::new(rest),
            processor,
            num_threads,
            start,
            limit,
        )
    }

    fn process_parallel_multi_interleaved<T>(
        self,
        arity: usize,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>,
    {
        process_parallel_generic(
            InterleavedMultiReader::new(self, arity),
            processor,
            num_threads,
        )
    }

    fn process_parallel_multi_interleaved_range<T>(
        self,
        arity: usize,
        processor: &mut T,
        num_threads: usize,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>,
    {
        let (start, limit) = range_to_offset_limit(range);
        process_parallel_generic_range(
            InterleavedMultiReader::new(self, arity),
            processor,
            num_threads,
            start,
            limit,
        )
    }
}

pub(crate) struct SingleReader<R: GenericReader> {
    reader: Mutex<R>,
}

impl<R: GenericReader> SingleReader<R> {
    pub fn new(reader1: R) -> Self {
        SingleReader {
            reader: Mutex::new(reader1),
        }
    }
}

impl<R: GenericReader> MTGenericReader for SingleReader<R>
where
    ProcessError: From<R::Error>,
{
    type RecordSet = R::RecordSet;
    type Error = ProcessError;
    type RefRecord<'a> = R::RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet {
        self.reader.lock().new_record_set()
    }

    fn fill(&self, record_set: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        let mut r1 = self.reader.lock();
        Ok(R::fill(&mut r1, record_set)?)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        R::iter(record_set).map(|r| Ok(r?))
    }

    fn n_records(record_set: &Self::RecordSet) -> usize {
        R::iter(record_set).len()
    }

    fn set_num_threads(&mut self, num_threads: usize) -> std::result::Result<(), Self::Error> {
        self.reader
            .lock()
            .set_threads(num_threads)
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::fastq;
    use crate::parallel::single::ParallelReader;
    use crate::parallel::{ParallelProcessor, ProcessError};
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
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel_range(&mut processor, 1, 10..20).unwrap();

        assert_eq!(processor.count(), 10);
    }

    #[test]
    fn test_range_basic_parallel() {
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel_range(&mut processor, 4, 10..20).unwrap();

        assert_eq!(processor.count(), 10);
    }

    #[test]
    fn test_range_from_start() {
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel_range(&mut processor, 4, 0..50).unwrap();

        assert_eq!(processor.count(), 50);
    }

    #[test]
    fn test_range_to_end() {
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel_range(&mut processor, 4, 450..).unwrap();

        assert_eq!(processor.count(), 50);
    }

    #[test]
    fn test_range_beyond_eof() {
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel_range(&mut processor, 4, 400..1000).unwrap();

        assert_eq!(processor.count(), 100);
    }

    #[test]
    fn test_range_empty() {
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel_range(&mut processor, 4, 100..100).unwrap();

        assert_eq!(processor.count(), 0);
    }

    #[test]
    fn test_range_non_batch_aligned() {
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel_range(&mut processor, 4, 17..83).unwrap();

        assert_eq!(processor.count(), 66);
    }

    #[test]
    fn test_range_single_batch() {
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel_range(&mut processor, 4, 15..22).unwrap();

        assert_eq!(processor.count(), 7);
    }

    #[test]
    fn test_range_inclusive() {
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel_range(&mut processor, 4, 10..=19).unwrap();

        assert_eq!(processor.count(), 10);
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

    impl<Rf: Record> super::PairedParallelProcessor<Rf> for PairedCountingProcessor {
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
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = PairedCountingProcessor::default();

        r1.process_parallel_paired_range(r2, &mut processor, 4, 10..30).unwrap();

        assert_eq!(processor.count(), 20);
    }

    #[test]
    fn test_range_paired_sequential() {
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = PairedCountingProcessor::default();

        r1.process_parallel_paired_range(r2, &mut processor, 1, 5..15).unwrap();

        assert_eq!(processor.count(), 10);
    }

    #[test]
    fn test_range_interleaved_basic() {
        let data = make_fastq(N_RECORDS);
        let reader = fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
        let mut processor = PairedCountingProcessor::default();

        reader.process_parallel_interleaved_range(&mut processor, 4, 10..30).unwrap();

        assert_eq!(processor.count(), 20); // 20 pairs (40 file records)
    }

    #[test]
    fn test_range_interleaved_from_start() {
        let data = make_fastq(N_RECORDS);
        let reader = fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
        let mut processor = PairedCountingProcessor::default();

        reader.process_parallel_interleaved_range(&mut processor, 4, 0..20).unwrap();

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

    impl<Rf: Record> super::MultiParallelProcessor<Rf> for MultiCountingProcessor {
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
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r3 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = MultiCountingProcessor::default();

        r1.process_parallel_multi_range(vec![r2, r3], &mut processor, 4, 10..30).unwrap();

        assert_eq!(processor.count(), 20);
    }

    #[test]
    fn test_range_multi_interleaved_basic() {
        // Process 10..30 = 20 record-groups (100 file records with arity=5)
        let data = make_fastq(N_RECORDS);
        let reader = fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
        let mut processor = MultiCountingProcessor::default();

        reader.process_parallel_multi_interleaved_range(5, &mut processor, 4, 10..30).unwrap();

        assert_eq!(processor.count(), 20); // 20 record-groups
    }

    #[test]
    fn test_range_multi_interleaved_from_start() {
        // Process 0..20 = 20 record-groups (40 file records with arity=2)
        let data = make_fastq(N_RECORDS);
        let reader = fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
        let mut processor = MultiCountingProcessor::default();

        reader.process_parallel_multi_interleaved_range(2, &mut processor, 4, 0..20).unwrap();

        assert_eq!(processor.count(), 20); // 20 record-groups
    }

    // Test that range semantics are consistent across modes
    #[test]
    fn test_range_semantic_consistency() {
        // All of these should process the same NUMBER of semantic units (50)
        // even though they read different numbers of file records

        // Single: 50 records from file
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p1 = CountingProcessor::default();
        r1.process_parallel_range(&mut p1, 4, 0..50).unwrap();
        assert_eq!(p1.count(), 50, "single-ended should process 50 records");

        // Paired: 50 pairs (50 records from each file = 100 total file records)
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p2 = PairedCountingProcessor::default();
        r1.process_parallel_paired_range(r2, &mut p2, 4, 0..50).unwrap();
        assert_eq!(p2.count(), 50, "paired should process 50 pairs");

        // Interleaved: 50 pairs (100 file records)
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p3 = PairedCountingProcessor::default();
        r1.process_parallel_interleaved_range(&mut p3, 4, 0..50).unwrap();
        assert_eq!(p3.count(), 50, "interleaved should process 50 pairs");

        // Multi (arity 2): 50 record-groups (50 records from each of 2 files = 100 total)
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let r2 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p4 = MultiCountingProcessor::default();
        r1.process_parallel_multi_range(vec![r2], &mut p4, 4, 0..50).unwrap();
        assert_eq!(p4.count(), 50, "multi should process 50 record-groups");

        // Multi-interleaved (arity 5): 50 record-groups (250 file records)
        // BATCH_SIZE=10 divides evenly by arity=5
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut p5 = MultiCountingProcessor::default();
        r1.process_parallel_multi_interleaved_range(5, &mut p5, 4, 0..50).unwrap();
        assert_eq!(p5.count(), 50, "multi-interleaved should process 50 record-groups");
    }
}
