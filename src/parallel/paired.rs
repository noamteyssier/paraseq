use itertools::Itertools;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::fastx::GenericReader;
use crate::parallel::error::{ProcessError, RecordPair};

use super::single::MTGenericReader;

pub struct PairedReader<R: GenericReader> {
    reader1: Mutex<R>,
    reader2: Mutex<R>,
    records_seen: AtomicUsize,
}

impl<R: GenericReader> PairedReader<R> {
    pub fn new(reader1: R, reader2: R) -> Self {
        PairedReader {
            reader1: Mutex::new(reader1),
            reader2: Mutex::new(reader2),
            records_seen: AtomicUsize::new(0),
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

    fn fill(
        &self,
        record_set: &mut Self::RecordSet,
    ) -> std::result::Result<Option<(usize, usize)>, Self::Error> {
        let mut r1 = self.reader1.lock();
        let filled_1 = R::fill(&mut r1, &mut record_set.0)?;

        if !filled_1 {
            // reader1 is exhausted. Still fill reader2 so a genuine
            // simultaneous EOF (both files the same length) can be told
            // apart from a length mismatch (reader2 still has records
            // reader1 doesn't) instead of silently discarding reader2's
            // leftovers.
            drop(r1);
            let mut r2 = self.reader2.lock();
            let filled_2 = R::fill(&mut r2, &mut record_set.1)?;
            drop(r2);
            return if filled_2 {
                Err(ProcessError::PairedRecordMismatch(RecordPair::R1))
            } else {
                Ok(None)
            };
        }

        let batch_size = R::iter(&record_set.0).len();
        let batch_start = self.records_seen.fetch_add(batch_size, Ordering::Relaxed);

        let mut r2 = self.reader2.lock();
        drop(r1);
        let filled_2 = R::fill(&mut r2, &mut record_set.1)?;
        drop(r2);

        if !filled_2 {
            // reader1 had a leftover batch reader2 doesn't have a match
            // for - a length mismatch, not ordinary EOF. Must error rather
            // than return `Ok(None)`, or that leftover batch is silently
            // dropped with no signal to the caller.
            return Err(ProcessError::PairedRecordMismatch(RecordPair::R2));
        }
        Ok(Some((batch_start, batch_start + batch_size)))
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

    fn set_num_threads(&mut self, num_threads: usize) -> std::result::Result<(), Self::Error> {
        self.reader1.lock().set_threads(num_threads)?;

        self.reader2.lock().set_threads(num_threads)?;

        Ok(())
    }
}

pub struct InterleavedPairedReader<R: GenericReader> {
    reader: Mutex<R>,
    records_seen: AtomicUsize,
}

impl<R: GenericReader> InterleavedPairedReader<R> {
    pub fn new(reader: R) -> Self {
        InterleavedPairedReader {
            reader: Mutex::new(reader),
            records_seen: AtomicUsize::new(0),
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

    fn fill(
        &self,
        record_set: &mut Self::RecordSet,
    ) -> std::result::Result<Option<(usize, usize)>, Self::Error> {
        let mut r = self.reader.lock();
        if !r.fill(record_set)? {
            return Ok(None);
        }
        // Batch position is in pairs, not individual records, to match
        // what `iter` below yields.
        let batch_size = {
            let n_records = R::iter(record_set).len();
            if !n_records.is_multiple_of(2) {
                return Err(ProcessError::IncompatibleInterleavedSetSize(n_records));
            }
            n_records / 2
        };
        let batch_start = self.records_seen.fetch_add(batch_size, Ordering::Relaxed);
        Ok(Some((batch_start, batch_start + batch_size)))
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        let it = R::iter(record_set);
        debug_assert!(
            it.len() % 2 == 0,
            "InterleavedPairedReader::iter called on an odd-length record set ({}); fill() should already have rejected this",
            it.len()
        );
        it.tuples()
            .map(|(r1, r2)| std::result::Result::Ok((r1?, r2?)))
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

        reader
            .process_parallel_interleaved(&mut processor, 1)
            .unwrap();

        assert_eq!(processor.count(), N_PAIRS);
    }

    #[test]
    fn test_interleaved_parallel() {
        let reader = fastq::Reader::new(Cursor::new(make_fastq(N_PAIRS * 2)));
        let mut processor = CountingPairProcessor::default();

        reader
            .process_parallel_interleaved(&mut processor, 4)
            .unwrap();

        assert_eq!(processor.count(), N_PAIRS);
    }

    #[test]
    fn test_paired_mismatched_sizes_errors() {
        let r1 = fastq::Reader::new(Cursor::new(make_fastq(200)));
        let r2 = fastq::Reader::new(Cursor::new(make_fastq(150)));
        let mut processor = CountingPairProcessor::default();

        let err = r1
            .process_parallel_paired(r2, &mut processor, 1)
            .unwrap_err();

        assert!(err.to_string().contains("Incompatible record set sizes"));
    }

    // Regression tests for a silent-data-loss bug: when one file runs out
    // of records a full batch (or more) before the other, `fill` used to
    // return `Ok(None)` - the same signal used for a clean, simultaneous
    // EOF - discarding the longer file's leftover records with no error.
    // These use a small batch size so the mismatch spans a batch boundary
    // (i.e. is only visible across separate `fill` calls), unlike
    // `test_paired_mismatched_sizes_errors` above, which mismatches within
    // a single batch and was already caught by `iter`.

    #[test]
    fn test_paired_length_mismatch_r2_shorter_errors() {
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(250)), 100).unwrap();
        let r2 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(200)), 100).unwrap();
        let mut processor = CountingPairProcessor::default();

        let err = r1
            .process_parallel_paired(r2, &mut processor, 1)
            .unwrap_err();

        assert!(err.to_string().contains("R2 has less records"));
        // The 200 matched pairs from the first two batches should still
        // have been delivered before the mismatch was detected.
        assert_eq!(processor.count(), 200);
    }

    #[test]
    fn test_paired_length_mismatch_r1_shorter_errors() {
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(200)), 100).unwrap();
        let r2 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(250)), 100).unwrap();
        let mut processor = CountingPairProcessor::default();

        let err = r1
            .process_parallel_paired(r2, &mut processor, 1)
            .unwrap_err();

        assert!(err.to_string().contains("R1 has less records"));
        assert_eq!(processor.count(), 200);
    }

    #[test]
    fn test_paired_length_mismatch_errors_parallel() {
        let r1 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(250)), 100).unwrap();
        let r2 = fastq::Reader::with_batch_size(Cursor::new(make_fastq(200)), 100).unwrap();
        let mut processor = CountingPairProcessor::default();

        let err = r1
            .process_parallel_paired(r2, &mut processor, 4)
            .unwrap_err();

        assert!(err.to_string().contains("has less records"));
    }
}
