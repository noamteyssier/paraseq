use itertools::Itertools;
use parking_lot::Mutex;

use crate::fastx::GenericReader;
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

impl<R: GenericReader> GenericReader for PairedReader<R>
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

    fn fill(&mut self, record_set: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        let mut r1 = self.reader1.lock();
        let mut r2 = self.reader2.lock();
        let filled1 = r1.fill(&mut record_set.0)?;
        let filled2 = r2.fill(&mut record_set.1)?;
        Ok(filled1 && filled2)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        let it1 = R::iter(&record_set.0);
        let it2 = R::iter(&record_set.1);

        // FIXME: Propagate an error instead.
        assert_eq!(
            it1.len(),
            it2.len(),
            "Record sets must have the same length"
        );
        std::iter::zip(it1, it2).map(|(r1, r2)| {
            let r1 = r1?;
            let r2 = r2?;
            R::check_read_pair(&r1, &r2)?;
            std::result::Result::Ok((r1, r2))
        })
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

impl<R: GenericReader> GenericReader for InterleavedPairedReader<R>
where
    ProcessError: From<R::Error>,
{
    type RecordSet = R::RecordSet;
    type Error = ProcessError;
    type RefRecord<'a> = (R::RefRecord<'a>, R::RefRecord<'a>);

    fn new_record_set(&self) -> Self::RecordSet {
        self.reader.lock().new_record_set()
    }

    fn fill(&mut self, record_set: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        // FIXME: ENSURE THIS READS AN EVEN NUMBER OF RECORDS.
        Ok(self.reader.lock().fill(record_set)?)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        let it = R::iter(record_set);
        // FIXME: Propagate an error instead.
        assert!(it.len() % 2 == 0, "Record set must have an even length");

        it.tuples()
            .map(|(r1, r2)| std::result::Result::Ok((r1?, r2?)))
    }
}
