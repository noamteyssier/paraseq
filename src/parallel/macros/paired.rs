use std::{io, sync::Arc, thread};

use crossbeam_channel::{bounded, Receiver, Sender};
use parking_lot::Mutex;

use crate::parallel::error::{ProcessError, RecordPair, Result};
use crate::parallel::{PairedParallelProcessor, PairedParallelReader};

/// Type alias for synchronized record sets containing pairs of records
type PairedRecordSets<T> = Arc<Vec<Mutex<(T, T)>>>;
/// Type alias for channels used in parallel processing
type ProcessorChannels = (Sender<Option<usize>>, Receiver<Option<usize>>);

/// Creates a collection of paired record sets for parallel processing
///
/// Note: The number of record sets is twice the number of threads
/// to allow for double buffering. Each set contains two records (R1 and R2)
fn create_paired_record_sets<T: Default>(num_threads: usize) -> PairedRecordSets<T> {
    let record_sets = (0..num_threads * 2)
        .map(|_| Mutex::new((T::default(), T::default())))
        .collect();
    Arc::new(record_sets)
}

/// Creates a pair of channels for communication between reader and worker threads
fn create_channels(buffer_size: usize) -> ProcessorChannels {
    bounded(buffer_size)
}

/// Internal processing of reader thread for paired reads
fn run_paired_reader_thread<R, T, F>(
    mut reader1: R,
    mut reader2: R,
    record_sets: PairedRecordSets<T>,
    tx: Sender<Option<usize>>,
    num_threads: usize,
    read_fn: F,
) -> Result<()>
where
    F: Fn(&mut R, &mut T) -> Result<bool>,
{
    let mut current_idx = 0;

    loop {
        let mut record_set_pair = record_sets[current_idx].lock();

        match (
            read_fn(&mut reader1, &mut record_set_pair.0),
            read_fn(&mut reader2, &mut record_set_pair.1),
        ) {
            (Ok(true), Ok(true)) => {
                drop(record_set_pair);
                tx.send(Some(current_idx))?;
                current_idx = (current_idx + 1) % record_sets.len();
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
        tx.send(None)?;
    }

    Ok(())
}

/// Internal processing of worker threads for paired reads
fn run_paired_worker_thread<T, P, F>(
    record_sets: PairedRecordSets<T>,
    rx: Receiver<Option<usize>>,
    mut processor: P,
    thread_id: usize,
    process_fn: F,
) -> Result<()>
where
    P: PairedParallelProcessor,
    F: Fn(&(T, T), &mut P) -> Result<()>,
{
    processor.set_thread_id(thread_id);
    while let Ok(Some(idx)) = rx.recv() {
        let record_set_pair = record_sets[idx].lock();
        process_fn(&record_set_pair, &mut processor)?;
        processor.on_batch_complete()?;
    }
    processor.on_thread_complete()?;
    Ok(())
}

/// Macro to implement parallel reader for paired reads
macro_rules! impl_paired_parallel_reader {
    ($reader:ty, $record_set:ty, $error:ty) => {
        impl<R> PairedParallelReader<R> for $reader
        where
            R: io::Read + Send,
        {
            fn process_parallel_paired<T>(
                self,
                reader2: Self,
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
                let record_sets = create_paired_record_sets::<$record_set>(num_threads);
                let (tx, rx) = create_channels(num_threads * 2);

                thread::scope(|scope| -> Result<()> {
                    // Spawn reader thread
                    let reader_sets = Arc::clone(&record_sets);
                    let reader_handle = scope.spawn(move || -> Result<()> {
                        run_paired_reader_thread(
                            self,
                            reader2,
                            reader_sets,
                            tx,
                            num_threads,
                            |reader, record_set| Ok(record_set.fill(reader)?),
                        )
                    });

                    // Spawn worker threads
                    let mut handles = Vec::new();
                    for thread_id in 0..num_threads {
                        let worker_sets = Arc::clone(&record_sets);
                        let worker_rx = rx.clone();
                        let worker_processor = processor.clone();

                        let handle = scope.spawn(move || {
                            run_paired_worker_thread(
                                worker_sets,
                                worker_rx,
                                worker_processor,
                                thread_id,
                                |record_set_pair, processor| {
                                    let mut records1 = record_set_pair.0.iter();
                                    let mut records2 = record_set_pair.1.iter();

                                    // Process records in pairs
                                    loop {
                                        match (records1.next(), records2.next()) {
                                            (Some(r1), Some(r2)) => {
                                                processor.process_record_pair(r1?, r2?)?;
                                            }
                                            (Some(_), None) => {
                                                // Record count mismatch between files // R2 has less records
                                                return Err(ProcessError::PairedRecordMismatch(
                                                    RecordPair::R2,
                                                ));
                                            }
                                            (None, Some(_)) => {
                                                // Record count mismatch between files // R1 has less records
                                                return Err(ProcessError::PairedRecordMismatch(
                                                    RecordPair::R1,
                                                ));
                                            }
                                            (None, None) => break,
                                        }
                                    }
                                    Ok(())
                                },
                            )
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
                let mut record_set1 = <$record_set>::default();
                let mut record_set2 = <$record_set>::default();

                loop {
                    match (
                        record_set1.fill(&mut reader1)?,
                        record_set2.fill(&mut reader2)?,
                    ) {
                        (true, true) => {
                            for (record1, record2) in record_set1.iter().zip(record_set2.iter()) {
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
    };
}

// Use the macro to implement for both FASTA and FASTQ
impl_paired_parallel_reader!(
    crate::fasta::Reader<R>,
    crate::fasta::RecordSet,
    crate::fasta::Error
);
impl_paired_parallel_reader!(
    crate::fastq::Reader<R>,
    crate::fastq::RecordSet,
    crate::fastq::Error
);
