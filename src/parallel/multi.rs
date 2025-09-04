use parking_lot::{Mutex, MutexGuard};
use smallvec::SmallVec;

use crate::fastx::{GenericReader, MTGenericReader};
use crate::parallel::error::ProcessError;
use crate::MAX_ARITY;

pub struct MultiReader<R: GenericReader> {
    readers: SmallVec<[Mutex<R>; MAX_ARITY]>,
}

impl<R: GenericReader> MultiReader<R> {
    pub fn new(readers: Vec<R>) -> Self {
        assert!(!readers.is_empty());
        Self {
            readers: readers.into_iter().map(Mutex::new).collect(),
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

    fn fill(&self, record_set: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        let mut prev_lock: Option<MutexGuard<_>> = None;

        let mut filled = None;

        for i in 0..self.readers.len() {
            let mut r = self.readers[i].lock();
            drop(prev_lock);
            let filledi = r.fill(&mut record_set[i])?;
            match filled {
                None => {
                    filled = Some(filledi);
                }
                Some(f) => {
                    if filledi != f {
                        return Err(ProcessError::MultiRecordMismatch(i));
                    }
                }
            }
            prev_lock = Some(r);
        }
        drop(prev_lock);
        Ok(filled.unwrap())
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
}

impl<R: GenericReader> InterleavedMultiReader<R> {
    pub fn new(reader: R, arity: usize) -> Self {
        assert!(arity > 0);
        Self {
            reader: Mutex::new(reader),
            arity,
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

    fn fill(&self, record_set: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        Ok(self.reader.lock().fill(&mut record_set.0)?)
    }

    fn iter(
        (record_set, arity): &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        let it = R::iter(record_set);
        if it.len() % arity != 0 {
            let err_iter = std::iter::once(Err(ProcessError::MultiRecordSetSizeMismatch(
                it.len(),
                *arity,
            )));
            return either::Either::Left(err_iter);
        }
        either::Either::Right(ChunkedIt { it, arity: *arity })
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
