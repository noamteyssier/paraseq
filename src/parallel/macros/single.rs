// use anyhow::{bail, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use parking_lot::Mutex;
use std::{io, sync::Arc, thread};

use crate::parallel::{error::Result, ParallelProcessor, ParallelReader, ProcessError};

type RecordSets<T> = Arc<Vec<Mutex<T>>>;
type ProcessorChannels = (Sender<Option<usize>>, Receiver<Option<usize>>);

/// Creates a collection of record sets
///
/// Note: The number of record sets is twice the number of threads
/// to allow for double buffering
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

/// Internal processing of reader thread
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

/// Internal processing of worker threads
fn run_worker_thread<T, P, F>(
    record_sets: RecordSets<T>,
    rx: Receiver<Option<usize>>,
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
        process_fn(&record_set, &mut processor)?;
        processor.on_batch_complete()?;
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
                            run_worker_thread(
                                worker_sets,
                                worker_rx,
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
                    reader_handle.join().unwrap()?;

                    // Wait for worker threads
                    for handle in handles {
                        handle.join().unwrap()?;
                    }

                    Ok(())
                })?;

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
