use std::thread;

use parking_lot::Mutex;

use crate::fastx::FastXReaderSupport;
use crate::parallel::error::{ProcessError, RecordPair, Result};
use crate::parallel::{PairedParallelProcessor, PairedParallelReader};
use crate::prelude::ParallelReader;
use crate::Record;

struct PairedEndReader<'r, R: FastXReaderSupport> {
    reader1: &'r Mutex<R>,
    reader2: &'r Mutex<R>,
}

impl<'r, R: FastXReaderSupport> FastXReaderSupport for PairedEndReader<'r, R>
{
    type RecordSet = (R::RecordSet, R::RecordSet);
    type Error = R::Error;
    type RefRecord<'a> = (R::RefRecord<'a>, R::RefRecord<'a>);

    fn new_record_set(&self) -> Self::RecordSet {
        (self.reader1.lock().new_record_set(), self.reader2.lock().new_record_set())
    }

    fn fill(&mut self, record_set: &mut Self::RecordSet) -> std::result::Result<bool, crate::Error> {
        let filled1 = self.reader1.lock().fill(&mut record_set.0)?;
        let filled2 = self.reader2.lock().fill(&mut record_set.1)?;
        Ok(filled1 && filled2)
    }

    fn iter(record_set: &Self::RecordSet) -> impl Iterator<Item = std::result::Result<Self::RefRecord<'_>, crate::Error>> {
        R::iter(&record_set.0)
            .zip(R::iter(&record_set.1))
            .map(|(r1, r2)| {
                let r1 = r1?;
                let r2 = r2?;
                Ok((r1, r2))
            })
    }
}

impl<S: FastXReaderSupport> PairedParallelReader for S
where
    S: ParallelReader,
    for <'a> S::RefRecord<'a>: Record
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
                        let s1 = r1.fill(&mut record_set_pair.0);
                        let mut r2 = reader2.lock();
                        drop(r1);
                        let s2 = r2.fill(&mut record_set_pair.1);
                        drop(r2);

                        match (s1?, s2?) {
                            (true, true) => {
                                // good case; record_set_pair is filled.
                            }
                            (true, false) => {
                                // Record count mismatch between files // R2 has less records
                                return Err(ProcessError::PairedRecordMismatch(RecordPair::R2));
                            }
                            (false, true) => {
                                // Record count mismatch between files // R1 has less records
                                return Err(ProcessError::PairedRecordMismatch(RecordPair::R1));
                            }
                            (false, false) => {
                                // Both files have reached EOF
                                break;
                            }
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
