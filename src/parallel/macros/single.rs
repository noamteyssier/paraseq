use std::io;
use std::thread;
use parking_lot::Mutex;
use crate::{fastx::FastXReaderSupport, parallel::{error::Result, ParallelProcessor, ParallelReader, ProcessError}, Record};

impl<S: FastXReaderSupport, R> ParallelReader<R> for S
where
    R: io::Read + Send,
    for <'a> S::RefRecord<'a>: Record
{
    fn process_parallel<T>(
        self,
        processor: T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: ParallelProcessor,
    {
        if num_threads == 0 {
            return Err(ProcessError::InvalidThreadCount);
        }
        if num_threads == 1 {
            return ParallelReader::<R>::process_sequential(self, processor);
        }

        let reader = Mutex::new(self);
        thread::scope(|scope| -> Result<()> {
            let reader = &reader;

            // Spawn worker threads
            let mut handles = Vec::new();
            for thread_id in 0..num_threads {
                let mut worker_processor = processor.clone();
                let mut record_set =                     reader.lock().new_record_set();

                let handle = scope.spawn(move || {
                    worker_processor.set_thread_id(thread_id);

                    loop {
                        let mut r1 = reader.lock();
                        let s1 = r1.fill(&mut record_set);
                        drop(r1);

                        if !s1? {
                                break;
                        }

                        let records = Self::iter(&record_set);

                        for record in records {
                            worker_processor.process_record(record?)?;
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

    fn process_sequential<T>(self, mut processor: T) -> Result<()>
    where
        T: ParallelProcessor,
    {
        let mut reader = self;
        let mut record_set = S::RecordSet::default();

        loop {
            match                 reader.fill(&mut record_set)?
 {
                true => {
                    for record in Self::iter(&record_set) {
                        processor.process_record(record?)?;
                    }
                    processor.on_batch_complete()?;
                }
                false => break,
            }
        }
        processor.on_thread_complete()?;
        Ok(())
    }
}
