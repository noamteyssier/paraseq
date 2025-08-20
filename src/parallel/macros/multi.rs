use std::{io, sync::Arc, thread};

use crossbeam_channel::{bounded, select, Receiver, Sender};
use parking_lot::Mutex;
use tinyvec::{array_vec, ArrayVec};

use crate::parallel::error::{ProcessError, Result};
use crate::parallel::{MultiParallelProcessor, MultiParallelReader};
use crate::MAX_ARITY;

/// Type alias for synchronized record sets containing arrays of records
type MultiRecordSets<T> = Arc<Vec<Mutex<ArrayVec<[T; MAX_ARITY]>>>>;
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

/// Internal processing of reader thread for paired reads
fn run_multi_reader_thread<R, T, F>(
    mut reader: R,
    readers: &mut [R],
    record_sets: MultiRecordSets<T>,
    tx: Sender<Option<usize>>,
    shutdown_rx: Receiver<()>,
    num_threads: usize,
    read_fn: F,
) -> Result<()>
where
    F: Fn(&mut R, &mut T) -> Result<bool>,
    T: std::default::Default,
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

        let mut record_set_elem = record_sets[current_idx].lock();
        let mut num_ok = 0;
        let mut num_ended = 0;
        let mut first_ended = usize::MAX;
        let record_set_elems = record_set_elem.as_mut_slice();

        let arity = 1 + readers.len();
        for i in 0..arity {
            let nreader = if i == 0 {
                &mut reader
            } else {
                &mut readers[i - 1]
            };
            let record = &mut record_set_elems[i];
            if let Ok(true) = read_fn(nreader, record) {
                num_ok += 1;
            } else {
                if first_ended == usize::MAX {
                    first_ended = i
                }
                num_ended += 1;
            }
        }
        if num_ok == arity {
            drop(record_set_elem);
            tx.send(Some(current_idx))?;
            current_idx = (current_idx + 1) % record_sets.len();
        } else if num_ended == arity {
            drop(record_set_elem);
            break;
        } else {
            return Err(ProcessError::MultiRecordMismatch(first_ended));
        }
    }

    // Signal completion to all workers
    for _ in 0..num_threads {
        let _ = tx.send(None); // Ignore errors as workers might have exited
    }

    Ok(())
}

/// Internal processing of worker threads for paired reads
fn run_multi_worker_thread<T, P, F>(
    record_sets: MultiRecordSets<T>,
    rx: Receiver<Option<usize>>,
    shutdown_tx: Sender<()>,
    mut processor: P,
    thread_id: usize,
    process_fn: F,
) -> Result<()>
where
    P: MultiParallelProcessor,
    F: Fn(&mut ArrayVec<[T; MAX_ARITY]>, &mut P) -> Result<()>,
{
    processor.set_thread_id(thread_id);
    while let Ok(Some(idx)) = rx.recv() {
        let mut record_set_multi = record_sets[idx].lock();
        if let Err(e) = process_fn(&mut record_set_multi, &mut processor) {
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

/// Macro to implement parallel reader for multi reads
macro_rules! impl_multi_parallel_reader {
    ($reader:ty, $record_set:ty, $record_t:ty, $error:ty) => {
        impl<R> MultiParallelReader for $reader
        where
            R: io::Read + Send,
        {
            fn process_parallel_multi<T>(
                self,
                mut remaining_readers: Vec<Self>,
                processor: T,
                num_threads: usize,
            ) -> Result<()>
            where
                T: MultiParallelProcessor,
                Self: std::marker::Sized,
            {
                if num_threads == 0 {
                    return Err(ProcessError::InvalidThreadCount);
                }
                if num_threads == 1 {
                    return self.process_sequential_multi(remaining_readers, processor);
                }
                let record_sets = Arc::new(
                    (0..num_threads * 2)
                        .map(|_| {
                            let mut record_set = ArrayVec::new();
                            record_set.push(self.new_record_set());
                            for r in remaining_readers.iter() {
                                record_set.push(r.new_record_set());
                            }
                            Mutex::new(record_set)
                        })
                        .collect::<Vec<_>>(),
                );
                let (tx, rx) = create_channels(num_threads * 2);
                let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

                thread::scope(|scope| -> Result<()> {
                    // Spawn reader thread
                    let reader_sets = Arc::clone(&record_sets);
                    let reader_handle = scope.spawn(move || -> Result<()> {
                        run_multi_reader_thread(
                            self,
                            remaining_readers.as_mut_slice(),
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
                            run_multi_worker_thread(
                                worker_sets,
                                worker_rx,
                                worker_shutdown_tx,
                                worker_processor,
                                thread_id,
                                |record_set_multi, processor| {
                                    let arity = record_set_multi.len();
                                    let mut iter_group = Vec::with_capacity(arity);
                                    for rs in record_set_multi.iter() {
                                        iter_group.push(rs.iter());
                                    }

                                    // Process records in groups
                                    'process_group_records: loop {
                                        let mut record_group = array_vec!([$record_t; MAX_ARITY]);
                                        let mut num_ok = 0;
                                        let mut num_empty = 0;
                                        let mut first_empty = usize::MAX;
                                        record_group.clear();
                                        // generate a group of records at a time by pulling out
                                        // the next available record from each of the iterators
                                        for (j, record_set_iter) in
                                            iter_group.iter_mut().enumerate()
                                        {
                                            if let Some(rec) = record_set_iter.next() {
                                                match rec {
                                                    Ok(rec) => {
                                                        record_group.push(rec);
                                                    }
                                                    Err(e) => {
                                                        return Err(ProcessError::FastxError(e));
                                                    }
                                                }
                                                num_ok += 1;
                                            } else {
                                                if first_empty == usize::MAX {
                                                    first_empty = j;
                                                }
                                                num_empty += 1;
                                            }
                                        }

                                        if num_ok == arity {
                                            processor
                                                .process_record_multi(record_group.as_slice())?;
                                        } else if num_empty == arity {
                                            break 'process_group_records;
                                        } else {
                                            return Err(ProcessError::MultiRecordMismatch(
                                                first_empty,
                                            ));
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

            fn process_sequential_multi<T>(
                mut self,
                mut remaining_readers: Vec<Self>,
                mut processor: T,
            ) -> Result<()>
            where
                T: MultiParallelProcessor,
                Self: std::marker::Sized,
            {
                let arity = 1 + remaining_readers.len();
                // this will hold the record set from each underlying reader
                let mut record_sets = array_vec!([$record_set; MAX_ARITY]);
                record_sets.push(self.new_record_set());
                for i in (0..remaining_readers.len()) {
                    record_sets.push(remaining_readers[i].new_record_set());
                }

                // until we run out of records to process
                loop {
                    // make sure we can refill each record set
                    let mut num_ok = 0;
                    let mut num_ended = 0;
                    let mut first_ended = usize::MAX;

                    // if one record set ends before the rest
                    // i.e. cannot be refilled while the others can
                    // then this is an error. Keep track of the first
                    // such record set.
                    for i in 0..arity {
                        let mut reader = if i == 0 {
                            &mut self
                        } else {
                            &mut remaining_readers[i - 1]
                        };
                        let did_fill = record_sets[i].fill(&mut reader)?;
                        if did_fill {
                            num_ok += 1;
                        } else {
                            if first_ended < usize::MAX {
                                first_ended = i;
                            }
                            num_ended += 1;
                        }
                    }
                    // if all of the record sets could be refilled
                    if num_ok == arity {
                        // get an iterator into each record set
                        let mut iter_group = Vec::with_capacity(arity);
                        let mut record_group = array_vec!([$record_t; MAX_ARITY]);
                        for rs in record_sets.iter() {
                            iter_group.push(rs.iter());
                        }

                        // walk through all record sets and get the next available
                        // record in each.
                        'process_group_records: loop {
                            let mut num_ok = 0;
                            let mut num_empty = 0;
                            let mut first_empty = usize::MAX;

                            // clear out the record group itself
                            record_group.clear();
                            for (j, record_set_iter) in iter_group.iter_mut().enumerate() {
                                if let Some(rec) = record_set_iter.next() {
                                    match rec {
                                        Ok(rec) => {
                                            record_group.push(rec);
                                        }
                                        Err(e) => {
                                            return Err(ProcessError::FastxError(e));
                                        }
                                    }
                                    num_ok += 1;
                                } else {
                                    if first_empty == usize::MAX {
                                        first_empty = j;
                                    }
                                    num_empty += 1;
                                }
                            }
                            if num_ok == arity {
                                processor.process_record_multi(record_group.as_slice())?;
                            } else if num_empty == arity {
                                break 'process_group_records;
                            } else {
                                return Err(ProcessError::MultiRecordMismatch(first_empty));
                            }
                        }
                        processor.on_batch_complete()?
                    } else if num_ended == arity {
                        break;
                    } else {
                        return Err(ProcessError::MultiRecordMismatch(first_ended));
                    }
                }
                processor.on_thread_complete()?;
                Ok(())
            }
        }
    };
}

// Use the macro to implement for both FASTA and FASTQ
impl_multi_parallel_reader!(
    crate::fasta::Reader<R>,
    crate::fasta::RecordSet,
    crate::fasta::RefRecord<'_>,
    crate::fasta::Error
);
impl_multi_parallel_reader!(
    crate::fastq::Reader<R>,
    crate::fastq::RecordSet,
    crate::fastq::RefRecord<'_>,
    crate::fastq::Error
);
