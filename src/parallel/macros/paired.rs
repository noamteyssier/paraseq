use std::{io, thread};

use crossbeam_channel::{bounded, select, Receiver, Sender};
use parking_lot::Mutex;

use crate::parallel::error::{ProcessError, RecordPair, Result};
use crate::parallel::{PairedParallelProcessor, PairedParallelReader};
use crate::prelude::ParallelReader;
use crate::Record;

/// Type alias for channels used in parallel processing
type ProcessorChannels = (Sender<Option<usize>>, Receiver<Option<usize>>);
type ShutdownChannel = (Sender<()>, Receiver<()>);

/// Creates a pair of channels for communication between reader and worker threads
fn create_channels(buffer_size: usize) -> ProcessorChannels {
    bounded(buffer_size)
}

/// Creates shutdown signal channel
fn create_shutdown_channel() -> ShutdownChannel {
    bounded(1)
}

trait PairedParallelReaderSupport: Send {
    type RecordSet: Default + Send;
    type Error;
    type RefRecord<'a>: Record;

    fn new_record_set(&self) -> Self::RecordSet;
    fn fill(&mut self, record: &mut Self::RecordSet) -> std::result::Result<bool, crate::Error>;

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl Iterator<Item = std::result::Result<Self::RefRecord<'_>, crate::Error>>;
}

impl<R> PairedParallelReaderSupport for crate::fasta::Reader<R>
where
    R: io::Read + Send,
{
    type RecordSet = crate::fasta::RecordSet;
    type Error = crate::Error;
    type RefRecord<'a> = crate::fasta::RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet {
        if let Some(batch_size) = self.batch_size {
            Self::RecordSet::new(batch_size)
        } else {
            Self::RecordSet::default()
        }
    }

    fn fill(&mut self, record: &mut Self::RecordSet) -> std::result::Result<bool, crate::Error> {
        record.fill(self)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl Iterator<Item = std::result::Result<Self::RefRecord<'_>, crate::Error>> {
        record_set
            .positions
            .iter()
            .map(move |&pos| Self::RefRecord::new(&record_set.buffer, pos))
    }
}

impl<R> PairedParallelReaderSupport for crate::fastq::Reader<R>
where
    R: io::Read + Send,
{
    type RecordSet = crate::fastq::RecordSet;
    type Error = crate::Error;
    type RefRecord<'a> = crate::fastq::RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet {
        if let Some(batch_size) = self.batch_size {
            Self::RecordSet::new(batch_size)
        } else {
            Self::RecordSet::default()
        }
    }

    fn fill(&mut self, record: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        record.fill(self)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl Iterator<Item = std::result::Result<Self::RefRecord<'_>, crate::Error>> {
        record_set
            .positions
            .iter()
            .map(move |&pos| Self::RefRecord::new(&record_set.buffer, pos))
    }
}

impl<S: PairedParallelReaderSupport, R> PairedParallelReader<R> for S
where
    S: ParallelReader<R>,
    R: io::Read + Send,
{
    fn process_parallel_paired<T>(
        self,
        mut reader2: Self,
        processor: T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: PairedParallelProcessor,
    {
        if num_threads == 0 {
            return Err(ProcessError::InvalidThreadCount);
        }
        if num_threads == 1 {
            return self.process_sequential_paired(reader2, processor);
        }

        let reader1 = Mutex::new(self);
        let reader2 = Mutex::new(reader2);
        thread::scope(|scope| -> Result<()> {
            let reader1 = &reader1;
            let reader2 = &reader2;

            // Spawn worker threads
            let mut handles = Vec::new();
            for thread_id in 0..num_threads {
                let mut worker_processor = processor.clone();
                let mut record_set_pair = (
                    reader1.lock().new_record_set(),
                    reader2.lock().new_record_set(),
                );

                let handle = scope.spawn(move || {
                    worker_processor.set_thread_id(thread_id);

                    loop {
                        let mut r1 = reader1.lock();
                        let mut r2 = reader2.lock();
                        let s1 = r1.fill(&mut record_set_pair.0);
                        let s2 = r2.fill(&mut record_set_pair.1);
                        drop(r1);
                        drop(r2);

                        match (s1, s2) {
                            (Ok(true), Ok(true)) => {
                                // good case; record_set_pair is filled.
                            }
                            (Ok(true), Ok(false)) => {
                                // Record count mismatch between files // R2 has less records
                                return Err(ProcessError::PairedRecordMismatch(RecordPair::R2));
                            }
                            (Ok(false), Ok(true)) => {
                                // Record count mismatch between files // R1 has less records
                                return Err(ProcessError::PairedRecordMismatch(RecordPair::R1));
                            }
                            _ => break, // EOF on either file
                        }

                        let mut records1 = Self::iter(&record_set_pair.0);
                        let mut records2 = Self::iter(&record_set_pair.1);

                        loop {
                            match (records1.next(), records2.next()) {
                                (Some(r1), Some(r2)) => {
                                    worker_processor.process_record_pair(r1?, r2?)?;
                                }
                                (Some(_), None) => {
                                    // Record count mismatch between files // R2 has less records
                                    return Err(ProcessError::PairedRecordMismatch(RecordPair::R2));
                                }
                                (None, Some(_)) => {
                                    // Record count mismatch between files // R1 has less records
                                    return Err(ProcessError::PairedRecordMismatch(RecordPair::R1));
                                }
                                (None, None) => break,
                            }
                        }

                        worker_processor.on_batch_complete()?;
                    }
                    worker_processor.on_thread_complete()?;
                    Ok(())
                });

                handles.push(handle);
            }

            // Wait for worker threads
            for handle in handles {
                match handle.join() {
                    Ok(Ok(())) => (),
                    Ok(Err(e)) => return Err(e),
                    Err(_) => return Err(ProcessError::JoinError),
                }
            }

            Ok(())
        })?;

        Ok(())
    }

    fn process_sequential_paired<T>(self, reader2: Self, mut processor: T) -> Result<()>
    where
        T: PairedParallelProcessor,
    {
        let mut reader1 = self;
        let mut reader2 = reader2;
        let mut record_set1 = S::RecordSet::default();
        let mut record_set2 = S::RecordSet::default();

        loop {
            match (
                reader1.fill(&mut record_set1)?,
                reader2.fill(&mut record_set2)?,
            ) {
                (true, true) => {
                    for (record1, record2) in
                        std::iter::zip(Self::iter(&record_set1), Self::iter(&record_set2))
                    {
                        let record1 = record1?;
                        let record2 = record2?;
                        processor.process_record_pair(record1, record2)?;
                    }
                    processor.on_batch_complete()?;
                }
                (true, false) => {
                    // Record count mismatch between files // R2 has less records
                    return Err(ProcessError::PairedRecordMismatch(RecordPair::R2));
                }
                (false, true) => {
                    // Record count mismatch between files // R1 has less records
                    return Err(ProcessError::PairedRecordMismatch(RecordPair::R1));
                }
                (false, false) => break,
            }
        }
        processor.on_thread_complete()?;
        Ok(())
    }
}
