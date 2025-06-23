use std::io;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{bounded, select, Receiver, Sender};
use parking_lot::Mutex;

use crate::parallel::{error::Result, ParallelProcessor, ParallelReader, ProcessError};

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

/// Internal processing of reader thread with shutdown signal
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
                // Shutdown signal received, stop reading
                break;
            }
            default => {}
        }

        let mut record_set = record_sets[current_idx].lock();
        match read_fn(&mut reader, &mut record_set) {
            Ok(true) => {
                drop(record_set);

                // Try to send with shutdown checking
                select! {
                    send(tx, Some(current_idx)) -> send_result => {
                        if send_result.is_err() {
                            // Channel closed, stop processing
                            break;
                        }
                        current_idx = (current_idx + 1) % record_sets.len();
                    }
                    recv(shutdown_rx) -> _ => {
                        // Shutdown signal received while sending
                        break;
                    }
                }
            }
            Ok(false) => break, // EOF
            Err(e) => return Err(e),
        }
    }

    // Signal completion to all workers
    for _ in 0..num_threads {
        let _ = tx.send(None); // Ignore errors as workers might have exited
    }

    Ok(())
}

/// Internal processing of worker threads with shutdown signaling
fn run_worker_thread<T, P, F>(
    record_sets: RecordSets<T>,
    rx: Receiver<Option<usize>>,
    shutdown_tx: Sender<()>,
    mut processor: P,
    thread_id: usize,
    process_fn: F,
) -> Result<()>
where
    P: ParallelProcessor,
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
            let _ = shutdown_tx.send(());
            return Err(e);
        }
    }

    processor.on_thread_complete()?;
    Ok(())
}

macro_rules! impl_parallel_reader {
    ($reader:ty, $record_set:ty, $error:ty) => {
        impl<R> ParallelReader<R> for $reader
        where
            R: io::Read + Send,
        {
            fn process_parallel<T>(self, processor: T, num_threads: usize) -> Result<()>
            where
                T: ParallelProcessor,
            {
                if num_threads == 0 {
                    return Err(ProcessError::InvalidThreadCount);
                }

                if num_threads == 1 {
                    return self.process_sequential(processor);
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
                            run_worker_thread(
                                worker_sets,
                                worker_rx,
                                worker_shutdown_tx,
                                worker_processor,
                                thread_id,
                                |record_set, processor| {
                                    for record in record_set.iter() {
                                        let record = record?;
                                        processor.process_record(record)?;
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

            fn process_sequential<T>(self, mut processor: T) -> Result<()>
            where
                T: ParallelProcessor,
            {
                let mut reader = self;
                let mut record_set = <$record_set>::default();

                while record_set.fill(&mut reader)? {
                    for record in record_set.iter() {
                        let record = record?;
                        processor.process_record(record)?;
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
impl_parallel_reader!(
    crate::fasta::Reader<R>,
    crate::fasta::RecordSet,
    crate::fasta::Error
);
impl_parallel_reader!(
    crate::fastq::Reader<R>,
    crate::fastq::RecordSet,
    crate::fastq::Error
);
