use std::borrow::Cow;
use std::sync::OnceLock;
use std::{io, path::Path};

use rust_htslib::bam::{self, Read as BamRead};
use thiserror::Error;

use crate::fastx::GenericReader;
use crate::{parallel::Result, ProcessError, Record};

/// Type alias for the internal reader type used by htslib
pub type HtslibReader = Box<dyn io::Read + Send>;

/// The size of the batch used for parallel processing.
pub const BATCH_SIZE: usize = 1024;

/// Error type for parallel htslib operations.
#[derive(Error, Debug)]
pub enum ParallelHtslibError {
    #[error("Record synchronization error for htslib files.")]
    PairedRecordMismatch,

    #[error("Unpaired record encountered: {0}")]
    UnpairedRecord(String),

    #[error("Paired records with different QNames encountered when expected: {0} != {1}")]
    PairedRecordsWithDifferentQNames(String, String),

    #[error("Paired records with same template position encountered when expected")]
    PairedRecordsWithSameTemplatePosition,
}

pub struct Reader(bam::Reader);
impl Reader {
    pub fn from_optional_path<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let inner = match path {
            Some(path) => bam::Reader::from_path(path)?,
            None => bam::Reader::from_stdin()?,
        };
        Ok(Self(inner))
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let inner = bam::Reader::from_path(path)?;
        Ok(Self(inner))
    }

    pub fn from_stdin() -> Result<Self> {
        let inner = bam::Reader::from_stdin()?;
        Ok(Self(inner))
    }

    pub fn from_reader(reader: bam::Reader) -> Self {
        Self(reader)
    }
}

pub struct RefRecord<'a> {
    pub inner: &'a bam::Record,
    /// Used to store ASCII-encoded quality scores as BAM records store raw PHRED scores.
    qual: OnceLock<Option<Vec<u8>>>,
}
impl<'a> RefRecord<'a> {
    pub fn new(record: &'a bam::Record) -> Self {
        Self {
            inner: record,
            qual: OnceLock::new(),
        }
    }
}

impl Record for RefRecord<'_> {
    fn id(&self) -> &[u8] {
        self.inner.qname()
    }
    fn seq(&self) -> Cow<'_, [u8]> {
        self.inner.seq().as_bytes().into()
    }
    fn seq_raw(&self) -> &[u8] {
        unimplemented!("seq_raw is unimplemented by htslib readers")
    }
    fn qual(&self) -> Option<&[u8]> {
        self.qual
            .get_or_init(|| {
                let qual = self.inner.qual();
                if qual.is_empty() {
                    None
                } else {
                    Some(qual.iter().map(|&phred| phred + 33).collect())
                }
            })
            .as_deref()
    }
}

impl GenericReader for Reader {
    // just one
    type RecordSet = bam::Record;
    type Error = ProcessError;
    type RefRecord<'a> = RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet {
        bam::Record::new()
    }

    fn fill(&mut self, record: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        match self.0.read(record) {
            Some(r) => {
                r?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        std::iter::once(Ok(RefRecord::new(record_set)))
    }

    fn check_read_pair(
        rec1: &Self::RefRecord<'_>,
        rec2: &Self::RefRecord<'_>,
    ) -> std::result::Result<(), Self::Error> {
        let rec1 = rec1.inner;
        let rec2 = rec2.inner;
        if !rec1.is_paired() {
            let qname = std::str::from_utf8(rec1.qname()).unwrap().to_string();
            return Err(ParallelHtslibError::UnpairedRecord(qname).into());
        }
        if !rec2.is_paired() {
            let qname = std::str::from_utf8(rec2.qname()).unwrap().to_string();
            return Err(ParallelHtslibError::UnpairedRecord(qname).into());
        }

        if rec1.qname() != rec2.qname() {
            return Err(ParallelHtslibError::PairedRecordsWithDifferentQNames(
                std::str::from_utf8(rec1.qname()).unwrap().to_string(),
                std::str::from_utf8(rec2.qname()).unwrap().to_string(),
            )
            .into());
        }

        if rec1.is_first_in_template() && rec2.is_first_in_template() {
            return Err(ParallelHtslibError::PairedRecordsWithSameTemplatePosition.into());
        }

        Ok(())
    }
}
