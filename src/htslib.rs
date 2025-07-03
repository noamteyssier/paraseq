use std::borrow::Cow;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::{io, path::Path};

use parking_lot::Mutex;
use rust_htslib::bam::{self, Read as BamRead};

use crate::{
    parallel::{ParallelReader, Result},
    ProcessError, Record,
};

/// Type alias for the internal reader type used by htslib
pub type HtslibReader = Box<dyn io::Read + Send>;

/// The size of the batch used for parallel processing.
pub const BATCH_SIZE: usize = 1024;

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
    fn seq(&self) -> Cow<[u8]> {
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
