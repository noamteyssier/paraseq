use crate::parallel::processor::GenericProcessor;
use crate::{
    fastx::GenericReader,
    parallel::{error::Result, ParallelReader, ProcessError},
};
use parking_lot::Mutex;
use std::thread;

impl<S: GenericReader> ParallelReader for S {
    type Rf<'a> = S::RefRecord<'a>;

    fn process_parallel<T>(mut self, processor: T, num_threads: usize) -> Result<()>
    where
        T: for<'a> GenericProcessor<S::RefRecord<'a>>,
    {
        if num_threads == 0 {
            return Err(ProcessError::InvalidThreadCount);
        }
        if num_threads == 1 {
            return ParallelReader::process_sequential(self, processor);
        }

        self.set_num_threads(num_threads);

        let reader = Mutex::new(self);
        thread::scope(|scope| -> Result<()> {
            let reader = &reader;

            // Spawn worker threads
            let mut handles = Vec::new();
            for thread_id in 0..num_threads {
                let mut worker_processor = processor.clone();
                let mut record_set = reader.lock().new_record_set();

                let handle = scope.spawn(move || {
                    worker_processor.set_thread_id(thread_id);

                    loop {
                        let mut r1 = reader.lock();
                        let s1 = r1.fill(&mut record_set);
                        drop(r1);

                        if !s1.map_err(Into::into)? {
                            break;
                        }

                        let records = Self::iter(&record_set);

                        for record in records {
                            worker_processor.process_record(record.map_err(Into::into)?)?;
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
        T: for<'a> GenericProcessor<S::RefRecord<'a>>,
    {
        let mut reader = self;
        let mut record_set = reader.new_record_set();

        loop {
            match reader.fill(&mut record_set).map_err(Into::into)? {
                true => {
                    for record in Self::iter(&record_set) {
                        processor.process_record(record.map_err(Into::into)?)?;
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
