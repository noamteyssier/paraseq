use itertools::Itertools;
use parking_lot::Mutex;

use crate::fastx::{GenericReader, MTGenericReader};
use crate::parallel::error::ProcessError;

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
}
