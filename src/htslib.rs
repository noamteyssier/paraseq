use std::borrow::Cow;
use std::sync::OnceLock;
use std::{io, path::Path};

use rust_htslib::bam::{self, Read as BamRead};
use thiserror::Error;

use crate::fastx::GenericReader;
use crate::validation::ValidationMode;
use crate::DEFAULT_MAX_RECORDS;
use crate::{
    parallel::{IntoProcessError, Result},
    ProcessError, Record,
};

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

pub struct Reader {
    reader: bam::Reader,
    batch_size: Option<usize>,
}
impl Reader {
    pub fn from_optional_path<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let inner = match path {
            Some(path) => bam::Reader::from_path(path)?,
            None => bam::Reader::from_stdin()?,
        };
        Ok(Self {
            reader: inner,
            batch_size: None,
        })
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let inner = bam::Reader::from_path(path)?;
        Ok(Self {
            reader: inner,
            batch_size: None,
        })
    }

    pub fn from_stdin() -> Result<Self> {
        let inner = bam::Reader::from_stdin()?;
        Ok(Self {
            reader: inner,
            batch_size: None,
        })
    }

    pub fn from_reader(reader: bam::Reader) -> Self {
        Self {
            reader,
            batch_size: None,
        }
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

#[derive(Clone)]
pub struct RecordSet {
    records: Vec<bam::Record>,
    n_records: usize,
}
impl Default for RecordSet {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_RECORDS)
    }
}
impl RecordSet {
    pub fn new(capacity: usize) -> Self {
        Self {
            records: vec![bam::Record::default(); capacity],
            n_records: 0,
        }
    }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Result<RefRecord<'_>>> {
        self.records
            .iter()
            .take(self.n_records)
            .map(RefRecord::new)
            .map(Ok)
    }
}

impl GenericReader for Reader {
    type RecordSet = RecordSet;
    type Error = ProcessError;
    type RefRecord<'a> = RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet {
        if let Some(batch_size) = self.batch_size {
            Self::RecordSet::new(batch_size)
        } else {
            Self::RecordSet::new(DEFAULT_MAX_RECORDS)
        }
    }

    fn fill(&mut self, record_set: &mut Self::RecordSet) -> Result<bool> {
        // reset the counter
        record_set.n_records = 0;

        // fill the record set
        for record in &mut record_set.records {
            if let Some(res) = self.reader.read(record) {
                res?;
                record_set.n_records += 1;
            } else {
                break;
            }
        }

        // false if reader is exhausted
        Ok(record_set.n_records > 0)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = Result<Self::RefRecord<'_>>> {
        record_set.iter()
    }

    fn check_read_pair(
        rec1: &Self::RefRecord<'_>,
        rec2: &Self::RefRecord<'_>,
        mode: ValidationMode,
    ) -> Result<()> {
        if matches!(mode, ValidationMode::Skip) {
            return Ok(());
        }

        let rec1 = rec1.inner;
        let rec2 = rec2.inner;
        if !rec1.is_paired() {
            let qname = std::str::from_utf8(rec1.qname()).unwrap().to_string();
            return Err(ParallelHtslibError::UnpairedRecord(qname).into_process_error());
        }
        if !rec2.is_paired() {
            let qname = std::str::from_utf8(rec2.qname()).unwrap().to_string();
            return Err(ParallelHtslibError::UnpairedRecord(qname).into_process_error());
        }

        if rec1.qname() != rec2.qname() {
            return Err(ParallelHtslibError::PairedRecordsWithDifferentQNames(
                std::str::from_utf8(rec1.qname()).unwrap().to_string(),
                std::str::from_utf8(rec2.qname()).unwrap().to_string(),
            )
            .into_process_error());
        }

        if rec1.is_first_in_template() && rec2.is_first_in_template() {
            return Err(
                ParallelHtslibError::PairedRecordsWithSameTemplatePosition.into_process_error()
            );
        }

        Ok(())
    }
}
