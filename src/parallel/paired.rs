use itertools::Itertools;
use parking_lot::Mutex;

use crate::fastx::GenericReader;
use crate::parallel::error::ProcessError;

use super::single::MTGenericReader;

pub struct PairedReader<R: GenericReader> {
    reader1: Mutex<R>,
    reader2: Mutex<R>,
}

impl<R: GenericReader> PairedReader<R> {
    pub fn new(reader1: R, reader2: R) -> Self {
        PairedReader {
            reader1: Mutex::new(reader1),
            reader2: Mutex::new(reader2),
        }
    }
}

impl<R: GenericReader> MTGenericReader for PairedReader<R>
where
    ProcessError: From<R::Error>,
{
    type RecordSet = (R::RecordSet, R::RecordSet);
    type Error = ProcessError;
    type RefRecord<'a> = (R::RefRecord<'a>, R::RefRecord<'a>);

    fn new_record_set(&self) -> Self::RecordSet {
        (
            self.reader1.lock().new_record_set(),
            self.reader2.lock().new_record_set(),
        )
    }

    fn fill(&self, record_set: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        let mut r1 = self.reader1.lock();
        let filled1 = R::fill(&mut r1, &mut record_set.0)?;
        let mut r2 = self.reader2.lock();
        drop(r1);
        let filled2 = R::fill(&mut r2, &mut record_set.1)?;
        drop(r2);
        Ok(filled1 && filled2)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        let it1 = R::iter(&record_set.0);
        let it2 = R::iter(&record_set.1);

        // incompatible record set sizes
        if it1.len() != it2.len() {
            let error_iter = std::iter::once(Err(ProcessError::IncompatibleRecordSetSizes(
                it1.len(),
                it2.len(),
            )));
            return either::Either::Left(error_iter);
        }

        let record_iter = std::iter::zip(it1, it2).map(|(r1, r2)| {
            let r1 = r1?;
            let r2 = r2?;
            R::check_read_pair(&r1, &r2)?;
            std::result::Result::Ok((r1, r2))
        });
        either::Either::Right(record_iter)
    }

    fn n_records(record_set: &Self::RecordSet) -> usize {
        R::iter(&record_set.0).len()
    }

    fn set_num_threads(&mut self, num_threads: usize) -> std::result::Result<(), Self::Error> {
        self.reader1.lock().set_threads(num_threads)?;

        self.reader2.lock().set_threads(num_threads)?;

        Ok(())
    }
}

pub struct InterleavedPairedReader<R: GenericReader> {
    reader: Mutex<R>,
}

impl<R: GenericReader> InterleavedPairedReader<R> {
    pub fn new(reader: R) -> Self {
        InterleavedPairedReader {
            reader: Mutex::new(reader),
        }
    }
}

impl<R: GenericReader> MTGenericReader for InterleavedPairedReader<R>
where
    ProcessError: From<R::Error>,
{
    type RecordSet = R::RecordSet;
    type Error = ProcessError;
    type RefRecord<'a> = (R::RefRecord<'a>, R::RefRecord<'a>);

    fn new_record_set(&self) -> Self::RecordSet {
        self.reader.lock().new_record_set()
    }

    fn fill(&self, record_set: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        // FIXME: ENSURE THIS READS AN EVEN NUMBER OF RECORDS.
        Ok(self.reader.lock().fill(record_set)?)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        let it = R::iter(record_set);

        if it.len() % 2 != 0 {
            let error_iter =
                std::iter::once(Err(ProcessError::IncompatibleInterleavedSetSize(it.len())));
            return either::Either::Left(error_iter);
        }

        let tuple_iter = it
            .tuples()
            .map(|(r1, r2)| std::result::Result::Ok((r1?, r2?)));
        either::Either::Right(tuple_iter)
    }

    fn n_records(record_set: &Self::RecordSet) -> usize {
        // Return the number of pairs, not individual records
        R::iter(record_set).len() / 2
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
    use crate::parallel::{PairedParallelProcessor, ParallelReader, ProcessError};
    use crate::Record;

    fn make_fastq(n: usize) -> Vec<u8> {
        (0..n)
            .flat_map(|i| format!("@seq{i}\nACGT\n+\nIIII\n").into_bytes())
            .collect()
    }

    #[derive(Clone, Default)]
    struct CountingPairProcessor {
        local_count: usize,
        global_count: Arc<AtomicUsize>,
    }
    impl CountingPairProcessor {
        fn count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }
    impl<Rf: Record> PairedParallelProcessor<Rf> for CountingPairProcessor {
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

    const N_PAIRS: usize = 200;

    #[test]
    fn test_paired_sequential() {
        let r1 = fastq::Reader::new(Cursor::new(make_fastq(N_PAIRS)));
        let r2 = fastq::Reader::new(Cursor::new(make_fastq(N_PAIRS)));
        let mut processor = CountingPairProcessor::default();

        r1.process_parallel_paired(r2, &mut processor, 1).unwrap();

        assert_eq!(processor.count(), N_PAIRS);
    }

    #[test]
    fn test_paired_parallel() {
        let r1 = fastq::Reader::new(Cursor::new(make_fastq(N_PAIRS)));
        let r2 = fastq::Reader::new(Cursor::new(make_fastq(N_PAIRS)));
        let mut processor = CountingPairProcessor::default();

        r1.process_parallel_paired(r2, &mut processor, 4).unwrap();

        assert_eq!(processor.count(), N_PAIRS);
    }

    #[test]
    fn test_interleaved_sequential() {
        let reader = fastq::Reader::new(Cursor::new(make_fastq(N_PAIRS * 2)));
        let mut processor = CountingPairProcessor::default();

        reader.process_parallel_interleaved(&mut processor, 1).unwrap();

        assert_eq!(processor.count(), N_PAIRS);
    }

    #[test]
    fn test_interleaved_parallel() {
        let reader = fastq::Reader::new(Cursor::new(make_fastq(N_PAIRS * 2)));
        let mut processor = CountingPairProcessor::default();

        reader.process_parallel_interleaved(&mut processor, 4).unwrap();

        assert_eq!(processor.count(), N_PAIRS);
    }

    #[test]
    fn test_paired_mismatched_sizes_errors() {
        let r1 = fastq::Reader::new(Cursor::new(make_fastq(200)));
        let r2 = fastq::Reader::new(Cursor::new(make_fastq(150)));
        let mut processor = CountingPairProcessor::default();

        let err = r1.process_parallel_paired(r2, &mut processor, 1).unwrap_err();

        assert!(err.to_string().contains("Incompatible record set sizes"));
    }
}
