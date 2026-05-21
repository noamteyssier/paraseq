use std::ops::RangeBounds;

use parking_lot::Mutex;

use crate::{
    fastx::GenericReader,
    parallel::{
        multi::{InterleavedMultiReader, MultiReader},
        paired::{InterleavedPairedReader, PairedReader},
        single::{process_parallel_generic, process_parallel_generic_range, MTGenericReader},
        MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor,
    },
    ProcessError, Record, Result,
};

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
        process_parallel_generic_range(MultiReader::new(rest), processor, num_threads, start, limit)
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

/// Helper to convert RangeBounds to (start, limit)
fn range_to_offset_limit(range: impl RangeBounds<usize>) -> (usize, Option<usize>) {
    use std::ops::Bound;

    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n + 1,
        Bound::Unbounded => 0,
    };

    let limit = match range.end_bound() {
        Bound::Included(&n) => Some((n + 1).max(start) - start),
        Bound::Excluded(&n) => Some(n.max(start) - start),
        Bound::Unbounded => None,
    };

    (start, limit)
}
