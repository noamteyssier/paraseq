use std::{io, sync::Arc, thread};

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
        let mut reader1 = self;

        if num_threads == 0 {
            return Err(ProcessError::InvalidThreadCount);
        }
        if num_threads == 1 {
            return reader1.process_sequential_paired(reader2, processor);
        }
        let record_sets = Arc::new(
            (0..num_threads * 2)
                .map(|_| Mutex::new((reader1.new_record_set(), reader2.new_record_set())))
                .collect::<Vec<_>>(),
        );
        let (tx, rx) = create_channels(num_threads * 2);
        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        thread::scope(|scope| -> Result<()> {
            // Spawn reader thread
            let reader_sets = Arc::clone(&record_sets);
            let reader_handle = scope.spawn(move || -> Result<()> {
                let mut current_idx = 0;

                loop {
                    // Check for shutdown signal before proceeding
                    select! {
                        recv(shutdown_rx) -> _ => {
                            // Shutdown signal received, stop reading
                            break;
                        }
                        default => {}
                    }

                    let mut record_set_pair = reader_sets[current_idx].lock();

                    match (
                        reader1.fill(&mut record_set_pair.0),
                        reader2.fill(&mut record_set_pair.1),
                    ) {
                        (Ok(true), Ok(true)) => {
                            drop(record_set_pair);
                            tx.send(Some(current_idx))?;
                            current_idx = (current_idx + 1) % reader_sets.len();
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
                }

                // Signal completion to all workers
                for _ in 0..num_threads {
                    let _ = tx.send(None); // Ignore errors as workers might have exited
                }

                Ok(())
            });

            // Spawn worker threads
            let mut handles = Vec::new();
            for thread_id in 0..num_threads {
                let worker_sets = Arc::clone(&record_sets);
                let worker_rx = rx.clone();
                let worker_shutdown_tx = shutdown_tx.clone();
                let mut worker_processor = processor.clone();

                let handle = scope.spawn(move || {
                    worker_processor.set_thread_id(thread_id);
                    while let Ok(Some(idx)) = worker_rx.recv() {
                        let record_set_pair = worker_sets[idx].lock();
                        let mut records1 = Self::iter(&record_set_pair.0);
                        let mut records2 = Self::iter(&record_set_pair.1);

                        let status = loop {
                            match (records1.next(), records2.next()) {
                                (Some(r1), Some(r2)) => {
                                    worker_processor.process_record_pair(r1?, r2?)?;
                                }
                                (Some(_), None) => {
                                    // Record count mismatch between files // R2 has less records
                                    break Err(ProcessError::PairedRecordMismatch(RecordPair::R2));
                                }
                                (None, Some(_)) => {
                                    // Record count mismatch between files // R1 has less records
                                    break Err(ProcessError::PairedRecordMismatch(RecordPair::R1));
                                }
                                (None, None) => break Ok(()),
                            }
                        };

                        if let Err(e) = status {
                            // Signal shutdown to reader thread
                            let _ = worker_shutdown_tx.send(());
                            return Err(e);
                        }

                        if let Err(e) = worker_processor.on_batch_complete() {
                            let _ = worker_shutdown_tx.send(());
                            return Err(e);
                        }
                    }
                    worker_processor.on_thread_complete()?;
                    Ok(())
                });

                handles.push(handle);
            }

            // Wait for reader thread
            match reader_handle.join() {
                Ok(Ok(())) => (),
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(ProcessError::JoinError),
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
