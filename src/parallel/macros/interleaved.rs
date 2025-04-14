use std::{io, sync::Arc, thread};

use crossbeam_channel::{bounded, Receiver, Sender};
use parking_lot::Mutex;

use crate::parallel::error::{ProcessError, RecordPair, Result};
use crate::parallel::{InterleavedParallelProcessor, InterleavedParallelReader};

type RecordSets<T> = Arc<Vec<Mutex<T>>>;
type ProcessorChannels = (Sender<Option<usize>>, Receiver<Option<usize>>);

/// Creates a collection of record sets for interleaved processing
fn create_record_sets<T: Default>(num_threads: usize) -> RecordSets<T> {
    let record_sets = (0..num_threads * 2)
        .map(|_| Mutex::new(T::default()))
        .collect();
    Arc::new(record_sets)
}

/// Creates a pair of channels for communication between reader and worker threads
fn create_channels(buffer_size: usize) -> ProcessorChannels {
    bounded(buffer_size)
}

/// Internal processing of reader thread for interleaved reads
fn run_reader_thread<R, T, F>(
    mut reader: R,
    record_sets: RecordSets<T>,
    tx: Sender<Option<usize>>,
    num_threads: usize,
    read_fn: F,
) -> Result<()>
where
    F: Fn(&mut R, &mut T) -> Result<bool>,
{
    let mut current_idx = 0;

    loop {
        let mut record_set = record_sets[current_idx].lock();
        if read_fn(&mut reader, &mut record_set)? {
            drop(record_set);
            tx.send(Some(current_idx))?;
            current_idx = (current_idx + 1) % record_sets.len();
        } else {
            break;
        }
    }

    // Signal completion
    for _ in 0..num_threads {
        tx.send(None)?;
    }

    Ok(())
}

/// Internal processing of worker threads for interleaved reads
fn run_interleaved_worker_thread<T, P, F>(
    record_sets: RecordSets<T>,
    rx: Receiver<Option<usize>>,
    mut processor: P,
    thread_id: usize,
    process_fn: F,
) -> Result<()>
where
    P: InterleavedParallelProcessor,
    F: Fn(&T, &mut P) -> Result<()>,
{
    processor.set_thread_id(thread_id);
    while let Ok(Some(idx)) = rx.recv() {
        let record_set = record_sets[idx].lock();
        process_fn(&record_set, &mut processor)?;
        processor.on_batch_complete()?;
    }
    processor.on_thread_complete()?;
    Ok(())
}

/// Macro to implement parallel reader for interleaved reads
macro_rules! impl_interleaved_parallel_reader {
    ($reader:ty, $record_set:ty, $error:ty) => {
        impl<R> InterleavedParallelReader<R> for $reader
        where
            R: io::Read + Send,
        {
            fn process_parallel_interleaved<T>(self, processor: T, num_threads: usize) -> Result<()>
            where
                T: InterleavedParallelProcessor,
            {
                if num_threads == 0 {
                    return Err(ProcessError::InvalidThreadCount);
                }

                if num_threads == 1 {
                    return self.process_sequential_interleaved(processor);
                }

                let record_sets = create_record_sets::<$record_set>(num_threads);
                let (tx, rx) = create_channels(num_threads * 2);

                thread::scope(|scope| -> Result<()> {
                    // Spawn reader thread
                    let reader_sets = Arc::clone(&record_sets);
                    let reader_handle = scope.spawn(move || -> Result<()> {
                        run_reader_thread(
                            self,
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
                            run_interleaved_worker_thread(
                                worker_sets,
                                worker_rx,
                                worker_processor,
                                thread_id,
                                |record_set, processor| {
                                    // Process records in interleaved pairs (R1, R2, R1, R2, ...)
                                    let mut records = record_set.iter().peekable();

                                    while records.peek().is_some() {
                                        // Get the first record (R1)
                                        let record1 = match records.next() {
                                            Some(r) => r?,
                                            None => break, // No more records
                                        };

                                        // Get the second record (R2)
                                        let record2 = match records.next() {
                                            Some(r) => r?,
                                            None => {
                                                // Odd number of records - incomplete pair
                                                return Err(ProcessError::PairedRecordMismatch(
                                                    RecordPair::R2,
                                                ));
                                            }
                                        };

                                        // Process the pair
                                        processor.process_interleaved_pair(record1, record2)?;
                                    }

                                    Ok(())
                                },
                            )
                        });

                        handles.push(handle);
                    }

                    // Wait for reader thread
                    match reader_handle.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => return Err(e),
                        Err(_) => return Err(ProcessError::JoinError),
                    }

                    // Wait for worker threads
                    for handle in handles {
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

            fn process_sequential_interleaved<T>(self, mut processor: T) -> Result<()>
            where
                T: InterleavedParallelProcessor,
            {
                let mut reader = self;
                let mut record_set = <$record_set>::default();

                while record_set.fill(&mut reader)? {
                    let mut records = record_set.iter();

                    loop {
                        // Get the first record (R1)
                        let record1 = match records.next() {
                            Some(r) => r?,
                            None => break, // No more records
                        };

                        // Get the second record (R2)
                        let record2 = match records.next() {
                            Some(r) => r?,
                            None => {
                                // Odd number of records - incomplete pair
                                return Err(ProcessError::PairedRecordMismatch(RecordPair::R2));
                            }
                        };

                        // Process the pair
                        processor.process_interleaved_pair(record1, record2)?;
                    }

                    processor.on_batch_complete()?;
                }

                processor.on_thread_complete()?;
                Ok(())
            }
        }
    };
}

// Use the macro to implement for both FASTA and FASTQ
impl_interleaved_parallel_reader!(
    crate::fasta::Reader<R>,
    crate::fasta::RecordSet,
    crate::fasta::Error
);
impl_interleaved_parallel_reader!(
    crate::fastq::Reader<R>,
    crate::fastq::RecordSet,
    crate::fastq::Error
);
