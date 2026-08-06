use parking_lot::{Mutex, MutexGuard};
use smallvec::SmallVec;

use crate::fastx::GenericReader;
use crate::parallel::error::ProcessError;
use crate::MAX_ARITY;

use super::single::{BatchCounter, MTGenericReader};

pub struct MultiReader<R: GenericReader> {
    readers: SmallVec<[Mutex<R>; MAX_ARITY]>,
    records_seen: BatchCounter,
}

impl<R: GenericReader> MultiReader<R> {
    pub fn new(readers: Vec<R>) -> Self {
        assert!(!readers.is_empty());
        Self {
            readers: readers.into_iter().map(Mutex::new).collect(),
            records_seen: BatchCounter::new(),
        }
    }
}

impl<R: GenericReader> MTGenericReader for MultiReader<R>
where
    ProcessError: From<R::Error>,
{
    type RecordSet = SmallVec<[R::RecordSet; MAX_ARITY]>;
    type Error = ProcessError;
    type RefRecord<'a> = SmallVec<[R::RefRecord<'a>; MAX_ARITY]>;

    fn new_record_set(&self) -> Self::RecordSet {
        self.readers
            .iter()
            .map(|r| r.lock().new_record_set())
            .collect()
    }

    fn fill(
        &self,
        record_set: &mut Self::RecordSet,
    ) -> std::result::Result<Option<(usize, usize)>, Self::Error> {
        let mut prev_lock: Option<MutexGuard<_>> = None;

        let mut filled = None;
        let mut claimed: Option<(usize, usize)> = None;

        for i in 0..self.readers.len() {
            let mut r = self.readers[i].lock();
            drop(prev_lock);
            let filled_i = r.fill(&mut record_set[i])?;
            match filled {
                None => {
                    filled = Some(filled_i);
                    if filled_i {
                        let batch_size = R::iter(&record_set[i]).len();
                        claimed = Some(self.records_seen.claim(batch_size));
                    }
                }
                Some(f) => {
                    if filled_i != f {
                        return Err(ProcessError::MultiRecordMismatch(i));
                    }
                }
            }
            prev_lock = Some(r);
        }
        drop(prev_lock);
        if !filled.unwrap() {
            return Ok(None);
        }
        Ok(claimed)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        let its: SmallVec<[_; MAX_ARITY]> = record_set.iter().map(|rs| R::iter(rs)).collect();
        if let Some(pos) = its.iter().position(|it| it.len() != its[0].len()) {
            let err_iter = std::iter::once(Err(ProcessError::MultiRecordMismatch(pos)));
            return either::Either::Left(err_iter);
        }
        either::Either::Right(SmallVecIt { its })
    }

    fn set_num_threads(&mut self, num_threads: usize) -> std::result::Result<(), Self::Error> {
        self.readers
            .iter()
            .try_for_each(|r| r.lock().set_threads(num_threads).map_err(Into::into))
    }
}

struct SmallVecIt<I> {
    its: SmallVec<[I; MAX_ARITY]>,
}

impl<Item, E: Into<ProcessError>, I: Iterator<Item = std::result::Result<Item, E>>> Iterator
    for SmallVecIt<I>
{
    type Item = std::result::Result<SmallVec<[Item; MAX_ARITY]>, ProcessError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut out = std::result::Result::Ok(SmallVec::default());
        for it in self.its.iter_mut() {
            // None early-breaks everything.
            let elem = it.next()?;
            // Err proceeds.
            if out.is_ok() {
                match elem {
                    Ok(it) => {
                        out.as_mut().unwrap().push(it);
                    }
                    Err(it) => {
                        out = std::result::Result::Err(it.into());
                    }
                }
            }
        }
        Some(out)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size_hint = self.its[0].size_hint();

        assert!(self.its.iter().all(|it| it.size_hint() == size_hint));
        size_hint
    }
}

impl<Item, E: Into<ProcessError>, I: ExactSizeIterator<Item = std::result::Result<Item, E>>>
    ExactSizeIterator for SmallVecIt<I>
{
}

pub struct InterleavedMultiReader<R: GenericReader> {
    reader: Mutex<R>,
    arity: usize,
    records_seen: BatchCounter,
}

impl<R: GenericReader> InterleavedMultiReader<R> {
    pub fn new(reader: R, arity: usize) -> Self {
        assert!(arity > 0);
        Self {
            reader: Mutex::new(reader),
            arity,
            records_seen: BatchCounter::new(),
        }
    }
}

impl<R: GenericReader> MTGenericReader for InterleavedMultiReader<R>
where
    ProcessError: From<R::Error>,
{
    type RecordSet = (R::RecordSet, usize);
    type Error = ProcessError;
    type RefRecord<'a> = SmallVec<[R::RefRecord<'a>; MAX_ARITY]>;

    fn new_record_set(&self) -> Self::RecordSet {
        (self.reader.lock().new_record_set(), self.arity)
    }

    fn fill(
        &self,
        record_set: &mut Self::RecordSet,
    ) -> std::result::Result<Option<(usize, usize)>, Self::Error> {
        let mut r = self.reader.lock();
        if !r.fill(&mut record_set.0)? {
            return Ok(None);
        }
        // Batch position is in record-groups, not individual records, to
        // match what `iter` below yields.
        let batch_size = {
            let n_records = R::iter(&record_set.0).len();
            if !n_records.is_multiple_of(self.arity) {
                // Same variant `iter` below raises for this condition, so
                // it doesn't matter which of the two catches a given batch
                // first - callers see one consistent error either way.
                return Err(ProcessError::MultiRecordSetSizeMismatch(
                    n_records, self.arity,
                ));
            }
            n_records / self.arity
        };
        Ok(Some(self.records_seen.claim(batch_size)))
    }

    fn iter(
        (record_set, arity): &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        let it = R::iter(record_set);
        debug_assert!(
            it.len() % arity == 0,
            "InterleavedMultiReader::iter called on a record set ({}) not a multiple of arity ({}); fill() should already have rejected this",
            it.len(),
            arity
        );
        ChunkedIt { it, arity: *arity }
    }

    fn set_num_threads(&mut self, num_threads: usize) -> std::result::Result<(), Self::Error> {
        self.reader
            .lock()
            .set_threads(num_threads)
            .map_err(Into::into)
    }
}

struct ChunkedIt<I> {
    it: I,
    arity: usize,
}

impl<Item, E: Into<ProcessError>, I: Iterator<Item = std::result::Result<Item, E>>> Iterator
    for ChunkedIt<I>
{
    type Item = std::result::Result<SmallVec<[Item; MAX_ARITY]>, ProcessError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut out = std::result::Result::Ok(SmallVec::default());
        for _ in 0..self.arity {
            // None early-breaks everything.
            let elem = self.it.next()?;
            // Err proceeds.
            if out.is_ok() {
                match elem {
                    Ok(it) => {
                        out.as_mut().unwrap().push(it);
                    }
                    Err(it) => {
                        out = std::result::Result::Err(it.into());
                    }
                }
            }
        }
        Some(out)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (x, y) = self.it.size_hint();
        (x / self.arity, y.map(|y| y / self.arity))
    }
}

impl<Item, E: Into<ProcessError>, I: ExactSizeIterator<Item = std::result::Result<Item, E>>>
    ExactSizeIterator for ChunkedIt<I>
{
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::fastq;
    use crate::parallel::{MultiParallelProcessor, ParallelReader, ProcessError};
    use crate::Record;

    fn make_fastq(n: usize) -> Vec<u8> {
        (0..n)
            .flat_map(|i| format!("@seq{i}\nACGT\n+\nIIII\n").into_bytes())
            .collect()
    }

    #[derive(Clone, Default)]
    struct CountingMultiProcessor {
        expected_arity: usize,
        local_count: usize,
        global_count: Arc<AtomicUsize>,
    }
    impl CountingMultiProcessor {
        fn new(expected_arity: usize) -> Self {
            Self {
                expected_arity,
                ..Default::default()
            }
        }
        fn count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }
    impl<Rf: Record> MultiParallelProcessor<Rf> for CountingMultiProcessor {
        fn process_multi_record(&mut self, records: &[Rf]) -> Result<(), ProcessError> {
            assert_eq!(records.len(), self.expected_arity);
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

    const N_GROUPS: usize = 100;

    fn readers(arity: usize) -> Vec<fastq::Reader<Cursor<Vec<u8>>>> {
        (0..arity)
            .map(|_| fastq::Reader::new(Cursor::new(make_fastq(N_GROUPS))))
            .collect()
    }

    #[test]
    fn test_multi_arity_2() {
        let mut rdrs = readers(2);
        let first = rdrs.remove(0);
        let mut processor = CountingMultiProcessor::new(2);

        first
            .process_parallel_multi(rdrs, &mut processor, 1)
            .unwrap();

        assert_eq!(processor.count(), N_GROUPS);
    }

    #[test]
    fn test_multi_arity_3_parallel() {
        let mut rdrs = readers(3);
        let first = rdrs.remove(0);
        let mut processor = CountingMultiProcessor::new(3);

        first
            .process_parallel_multi(rdrs, &mut processor, 4)
            .unwrap();

        assert_eq!(processor.count(), N_GROUPS);
    }

    #[test]
    fn test_multi_arity_4() {
        let mut rdrs = readers(4);
        let first = rdrs.remove(0);
        let mut processor = CountingMultiProcessor::new(4);

        first
            .process_parallel_multi(rdrs, &mut processor, 1)
            .unwrap();

        assert_eq!(processor.count(), N_GROUPS);
    }

    #[test]
    fn test_multi_interleaved_arity_2() {
        let reader = fastq::Reader::new(Cursor::new(make_fastq(N_GROUPS * 2)));
        let mut processor = CountingMultiProcessor::new(2);

        reader
            .process_parallel_multi_interleaved(2, &mut processor, 1)
            .unwrap();

        assert_eq!(processor.count(), N_GROUPS);
    }

    #[test]
    fn test_multi_interleaved_arity_3_parallel() {
        let reader = fastq::Reader::new(Cursor::new(make_fastq(N_GROUPS * 3)));
        let mut processor = CountingMultiProcessor::new(3);

        reader
            .process_parallel_multi_interleaved(3, &mut processor, 4)
            .unwrap();

        assert_eq!(processor.count(), N_GROUPS);
    }

    #[test]
    fn test_multi_mismatched_sizes_errors() {
        let r1 = fastq::Reader::new(Cursor::new(make_fastq(200)));
        let r2 = fastq::Reader::new(Cursor::new(make_fastq(150)));
        let mut processor = CountingMultiProcessor::new(2);

        let err = r1
            .process_parallel_multi(vec![r2], &mut processor, 1)
            .unwrap_err();

        assert!(err.to_string().contains("has fewer records"));
    }

    #[test]
    fn test_multi_interleaved_arity_mismatch_errors() {
        // Not a multiple of the requested arity
        for arity in 3..=5 {
            let reader = fastq::Reader::new(Cursor::new(make_fastq(N_GROUPS * arity + 1)));
            let mut processor = CountingMultiProcessor::new(arity);

            let err = reader
                .process_parallel_multi_interleaved(arity, &mut processor, 1)
                .unwrap_err();

            assert!(err
                .to_string()
                .contains(&format!("must be divisible by {}", arity)));
        }
    }
}
