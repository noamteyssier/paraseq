use std::sync::Arc;
use std::thread;
use std::{io, path::Path};

use parking_lot::Mutex;
use rust_htslib::bam::{self, Read as BamRead};

use crate::{
    parallel::{ParallelReader, Result},
    ProcessError, Record,
};

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

impl<R: io::Read + Send> ParallelReader<R> for Reader {
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
                        worker_processor.process_record(&record)?;

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
            processor.process_record(&record)?;
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

impl Record for bam::Record {
    fn id(&self) -> &[u8] {
        self.qname()
    }
    fn seq(&self) -> std::borrow::Cow<[u8]> {
        self.seq().as_bytes().into()
    }
    fn seq_raw(&self) -> &[u8] {
        unimplemented!("seq_raw is unimplemented for htslib")
    }
    fn qual(&self) -> Option<&[u8]> {
        if self.qual().is_empty() {
            None
        } else {
            Some(self.qual())
        }
    }
}
impl Record for &bam::Record {
    fn id(&self) -> &[u8] {
        self.qname()
    }
    fn seq(&self) -> std::borrow::Cow<[u8]> {
        (*self).seq().as_bytes().into()
    }
    fn seq_raw(&self) -> &[u8] {
        unimplemented!("seq_raw is unimplemented for htslib")
    }
    fn qual(&self) -> Option<&[u8]> {
        if (*self).qual().is_empty() {
            None
        } else {
            Some((*self).qual())
        }
    }
}
