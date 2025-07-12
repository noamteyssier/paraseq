use std::{io, sync::Arc, thread};

use crossbeam_channel::{bounded, select, Receiver, Sender};
use parking_lot::Mutex;
use tinyvec::array_vec;

use crate::parallel::error::{ProcessError, Result};
use crate::parallel::{InterleavedMultiParallelProcessor, InterleavedMultiParallelReader};
use crate::MAX_ARITY;

type RecordSets<T> = Arc<Vec<Mutex<T>>>;
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

/// Internal processing of reader thread for interleaved reads
fn run_reader_thread<R, T, F>(
    mut reader: R,
    record_sets: RecordSets<T>,
    tx: Sender<Option<usize>>,
    shutdown_rx: Receiver<()>,
    num_threads: usize,
    read_fn: F,
) -> Result<()>
where
    F: Fn(&mut R, &mut T) -> Result<bool>,
{
    let mut current_idx = 0;

    loop {
        // Check for shutdown signal before proceeding
        select! {
            recv(shutdown_rx) -> _ => {
                // Shutdown signal received, exit loop
                break;
            }
            default => {}
        }

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
        let _ = tx.send(None); // Ignore result since workers may have already exited
    }

    Ok(())
}

/// Internal processing of worker threads for interleaved reads
fn run_interleaved_worker_thread<T, P, F>(
    record_sets: RecordSets<T>,
    rx: Receiver<Option<usize>>,
    shutdown_tx: Sender<()>,
    mut processor: P,
    thread_id: usize,
    process_fn: F,
) -> Result<()>
where
    P: InterleavedMultiParallelProcessor,
    F: Fn(&T, &mut P) -> Result<()>,
{
    processor.set_thread_id(thread_id);
    while let Ok(Some(idx)) = rx.recv() {
        let record_set = record_sets[idx].lock();
        if let Err(e) = process_fn(&record_set, &mut processor) {
            // Signal shutdown to reader thread
            let _ = shutdown_tx.send(());
            return Err(e);
        }
        if let Err(e) = processor.on_batch_complete() {
            // Signal shutdown to reader thread
            let _ = shutdown_tx.send(());
            return Err(e);
        }
    }
    processor.on_thread_complete()?;
    Ok(())
}

/// Macro to implement parallel reader for interleaved reads
macro_rules! impl_interleaved_parallel_reader {
    ($reader:ty, $record_set:ty, $record_t: ty, $error:ty) => {
        impl<R> InterleavedMultiParallelReader<R> for $reader
        where
            R: io::Read + Send,
        {
            fn process_parallel_interleaved_multi<T>(
                self,
                arity: usize,
                processor: T,
                num_threads: usize,
            ) -> Result<()>
            where
                T: InterleavedMultiParallelProcessor,
            {
                if num_threads == 0 {
                    return Err(ProcessError::InvalidThreadCount);
                }

                if num_threads == 1 {
                    return self.process_sequential_interleaved_multi(arity, processor);
                }

                let record_sets = Arc::new(
                    (0..num_threads * 2)
                        .map(|_| Mutex::new(self.new_record_set()))
                        .collect::<Vec<_>>(),
                );
                let (tx, rx) = create_channels(num_threads * 2);
                let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

                thread::scope(|scope| -> Result<()> {
                    // Spawn reader thread
                    let reader_sets = Arc::clone(&record_sets);
                    let reader_handle = scope.spawn(move || -> Result<()> {
                        run_reader_thread(
                            self,
                            reader_sets,
                            tx,
                            shutdown_rx,
                            num_threads,
                            |reader, record_set| Ok(record_set.fill(reader)?),
                        )
                    });

                    // Spawn worker threads
                    let mut handles = Vec::new();
                    for thread_id in 0..num_threads {
                        let worker_sets = Arc::clone(&record_sets);
                        let worker_rx = rx.clone();
                        let worker_shutdown_tx = shutdown_tx.clone();
                        let worker_processor = processor.clone();

                        let handle = scope.spawn(move || {
                            run_interleaved_worker_thread(
                                worker_sets,
                                worker_rx,
                                worker_shutdown_tx,
                                worker_processor,
                                thread_id,
                                |record_set, processor| {
                                    // Process records in interleaved pairs (R1, R2, R1, R2, ...)
                                    let mut records = record_set.iter().peekable();
                                    let mut rec_vec = array_vec!([$record_t; MAX_ARITY]);
                                    while records.peek().is_some() {
                                        rec_vec.clear();
                                        for rank in 0..arity {
                                            // Get the next record
                                            let record = match records.next() {
                                                Some(r) => r?,
                                                None => {
                                                    if rank == 0 {
                                                        break;
                                                    } else {
                                                        return Err(
                                                            ProcessError::MultiRecordMismatch(rank),
                                                        );
                                                    }
                                                } // No more records
                                            };
                                            rec_vec.push(record);
                                        }

                                        // Process the pair
                                        processor
                                            .process_interleaved_multi(rec_vec.as_mut_slice())?;
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

            fn process_sequential_interleaved_multi<T>(
                self,
                arity: usize,
                mut processor: T,
            ) -> Result<()>
            where
                T: InterleavedMultiParallelProcessor,
            {
                let mut reader = self;
                let mut record_set = <$record_set>::default();

                while record_set.fill(&mut reader)? {
                    let mut records = record_set.iter();
                    let mut rec_vec = array_vec!([$record_t; MAX_ARITY]);
                    'process_reads: loop {
                        rec_vec.clear();
                        for rank in 0..arity {
                            // Get the next record
                            let record = match records.next() {
                                Some(r) => r?,
                                None => {
                                    if rank == 0 {
                                        break 'process_reads;
                                    } else {
                                        return Err(ProcessError::MultiRecordMismatch(rank));
                                    }
                                } // No more records
                            };
                            rec_vec.push(record);
                        }
                        // Process the group
                        processor.process_interleaved_multi(rec_vec.as_mut_slice())?;
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
    crate::fasta::RefRecord<'_>,
    crate::fasta::Error
);
impl_interleaved_parallel_reader!(
    crate::fastq::Reader<R>,
    crate::fastq::RecordSet,
    crate::fastq::RefRecord<'_>,
    crate::fastq::Error
);
