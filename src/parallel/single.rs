use itertools::Itertools;
use parking_lot::Mutex;

use crate::fastx::GenericReader;
use crate::parallel::processor::GenericProcessor;
use crate::parallel::{error::Result, ProcessError};
use crate::Record;
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

    fn set_num_threads(&mut self, _num_threads: usize) {}
    fn new_record_set(&self) -> Self::RecordSet;
    fn fill(&self, record: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error>;
    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>>;
}

fn process_sequential_generic<S: MTGenericReader, T>(reader: S, processor: &mut T) -> Result<()>
where
    T: for<'a> GenericProcessor<S::RefRecord<'a>>,
{
    let mut record_set = reader.new_record_set();

    while reader.fill(&mut record_set).map_err(Into::into)? {
        let records = S::iter(&record_set).map(|r| r.map_err(Into::into));

        // One ? for record parsing errors, and one ? for errors from worker_processor.
        records.process_results(|records| processor.process_record_batch(records))??;

        processor.on_batch_complete()?;
    }
    processor.on_thread_complete()?;
    Ok(())
}

pub(crate) fn process_parallel_generic<S: MTGenericReader, T>(
    mut reader: S,
    processor: &mut T,
    mut num_threads: usize,
) -> Result<()>
where
    T: for<'a> GenericProcessor<S::RefRecord<'a>>,
{
    if num_threads == 0 {
        num_threads = num_cpus::get();
    }
    if num_threads == 1 {
        return process_sequential_generic(reader, processor);
    }

    reader.set_num_threads(num_threads);

    thread::scope(|scope| -> Result<()> {
        let reader = &reader;

        // Spawn worker threads
        let mut handles = Vec::new();
        for thread_id in 0..num_threads {
            let mut worker_processor = processor.clone();
            let mut record_set = reader.new_record_set();

            let handle = scope.spawn(move || {
                worker_processor.set_thread_id(thread_id);

                loop {
                    let s1 = reader.fill(&mut record_set);

                    if !s1.map_err(Into::into)? {
                        break;
                    }

                    let records = S::iter(&record_set).map(|r| r.map_err(Into::into));

                    // One ? for record parsing errors, and one ? for errors from worker_processor.
                    records.process_results(|records| {
                        worker_processor.process_record_batch(records)
                    })??;

                    worker_processor.on_batch_complete()?;
                }
                worker_processor.on_thread_complete()?;
                Ok(())
            });

            handles.push(handle);
        }

        // Wait for worker threads
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

    fn process_parallel_paired<T>(
        self,
        r2: Self,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_interleaved<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
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

    fn process_parallel_multi_interleaved<T>(
        self,
        arity: usize,
        processor: &mut T,
        num_threads: usize,
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

    fn process_parallel_interleaved<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>,
    {
        process_parallel_generic(InterleavedPairedReader::new(self), processor, num_threads)
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
        let mut reader =
            fastq::Reader::with_batch_size(Cursor::new(data), BATCH_SIZE).unwrap();
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
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
        let mut processor = CountingProcessor::default();

        reader.process_parallel(&mut processor, 1).unwrap();

        assert_eq!(processor.count(), N_RECORDS);
    }

    #[test]
    fn test_no_limit_processes_all_parallel() {
        let reader = fastq::Reader::with_batch_size(Cursor::new(make_fastq(N_RECORDS)), BATCH_SIZE).unwrap();
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
}
