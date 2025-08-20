use std::borrow::Cow;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::{io, path::Path};

use parking_lot::Mutex;
use rust_htslib::bam::{self, Read as BamRead};
use thiserror::Error;

use crate::{
    parallel::{InterleavedParallelReader, IntoProcessError, ParallelReader, Result},
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
impl<'a> Record for RefRecord<'a> {
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

impl ParallelReader<HtslibReader> for Reader {
    fn process_parallel<T>(mut self, processor: T, num_threads: usize) -> Result<()>
    where
        T: crate::prelude::ParallelProcessor,
    {
        self.0.set_threads(num_threads)?;

        let shared_reader = Arc::new(Mutex::new(self));
        thread::scope(|scope| -> Result<()> {
            let mut worker_handles = Vec::new();
            for tid in 0..num_threads {
                let mut worker_processor = processor.clone();
                let thread_reader = shared_reader.clone();
                let worker_handle = scope.spawn(move || -> Result<()> {
                    worker_processor.set_thread_id(tid);
                    let mut record = bam::Record::new();
                    let mut n_records = 0;
                    while let Some(res) = thread_reader.lock().0.read(&mut record) {
                        res?;
                        let ref_record = RefRecord::new(&record);
                        worker_processor.process_record(ref_record)?;

                        if n_records == BATCH_SIZE {
                            worker_processor.on_batch_complete()?;
                            n_records = 0;
                        }
                        n_records += 1;
                    }

                    // Run on final batch complete
                    if n_records > 0 {
                        worker_processor.on_batch_complete()?;
                    }

                    // Run on thread complete
                    worker_processor.on_thread_complete()?;
                    Ok(())
                });
                worker_handles.push(worker_handle);
            }

            // Wait for all worker threads to finish
            for handle in worker_handles {
                match handle.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => return Err(e),
                    Err(_) => return Err(ProcessError::JoinError),
                }
            }
            Ok(())
        })?;

        Ok(())
    }

    fn process_sequential<T>(mut self, mut processor: T) -> Result<()>
    where
        T: crate::prelude::ParallelProcessor,
    {
        let mut record = bam::Record::new();
        let mut n_records = 0;
        while let Some(res) = self.0.read(&mut record) {
            res?;
            let ref_record = RefRecord::new(&record);
            processor.process_record(ref_record)?;
            n_records += 1;
            if n_records == BATCH_SIZE {
                processor.on_batch_complete()?;
                n_records = 0;
            }
        }
        if n_records > 0 {
            processor.on_batch_complete()?;
        }
        processor.on_thread_complete()?;
        Ok(())
    }
}

impl InterleavedParallelReader<HtslibReader> for Reader {
    fn process_parallel_interleaved<T>(mut self, processor: T, num_threads: usize) -> Result<()>
    where
        T: crate::prelude::InterleavedParallelProcessor,
    {
        self.0.set_threads(num_threads)?;

        if num_threads == 1 {
            return self.process_sequential_interleaved(processor);
        }

        let shared_reader = Arc::new(Mutex::new(self));
        thread::scope(|scope| -> Result<()> {
            let mut worker_handles = Vec::new();
            for tid in 0..num_threads {
                let mut worker_processor = processor.clone();
                let thread_reader = shared_reader.clone();
                let worker_handle = scope.spawn(move || -> Result<()> {
                    worker_processor.set_thread_id(tid);
                    let mut rec1 = bam::Record::new();
                    let mut rec2 = bam::Record::new();
                    let mut n_pairs = 0;
                    loop {
                        let (res1, res2) = {
                            let mut reader = thread_reader.lock();
                            let Some(res1) = reader.0.read(&mut rec1) else {
                                break;
                            };
                            let Some(res2) = reader.0.read(&mut rec2) else {
                                return Err(
                                    ParallelHtslibError::PairedRecordMismatch.into_process_error()
                                );
                            };
                            (res1, res2)
                        }; // drop lock on reader

                        res1?;
                        res2?; // handle errors

                        error_check_read_pairs(&rec1, &rec2)?;

                        let ref_rec1 = RefRecord::new(&rec1);
                        let ref_rec2 = RefRecord::new(&rec2);
                        worker_processor.process_interleaved_pair(ref_rec1, ref_rec2)?;

                        if n_pairs == BATCH_SIZE {
                            worker_processor.on_batch_complete()?;
                            n_pairs = 0;
                        }

                        n_pairs += 1;
                    }

                    if n_pairs > 0 {
                        worker_processor.on_batch_complete()?;
                    }

                    worker_processor.on_thread_complete()?;

                    Ok(())
                });
                worker_handles.push(worker_handle);
            }
            for handle in worker_handles {
                handle.join().unwrap()?;
            }
            Ok(())
        })?;
        Ok(())
    }

    fn process_sequential_interleaved<T>(mut self, mut processor: T) -> Result<()>
    where
        T: crate::prelude::InterleavedParallelProcessor,
    {
        let mut rec1 = bam::Record::new();
        let mut rec2 = bam::Record::new();
        let mut n_pairs = 0;
        loop {
            let Some(res1) = self.0.read(&mut rec1) else {
                break;
            };
            let Some(res2) = self.0.read(&mut rec2) else {
                return Err(ParallelHtslibError::PairedRecordMismatch.into_process_error());
            };

            res1?;
            res2?; // handle potential errors

            // validate record pairs
            error_check_read_pairs(&rec1, &rec2)?;

            let ref_record1 = RefRecord::new(&rec1);
            let ref_record2 = RefRecord::new(&rec2);
            processor.process_interleaved_pair(ref_record1, ref_record2)?;

            if n_pairs == BATCH_SIZE {
                processor.on_batch_complete()?;
                n_pairs = 0;
            }

            n_pairs += 1;
        }
        if n_pairs > 0 {
            processor.on_batch_complete()?;
        }
        processor.on_thread_complete()?;
        Ok(())
    }
}

fn error_check_read_pairs(rec1: &bam::Record, rec2: &bam::Record) -> Result<()> {
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
        return Err(ParallelHtslibError::PairedRecordsWithSameTemplatePosition.into_process_error());
    }

    Ok(())
}
