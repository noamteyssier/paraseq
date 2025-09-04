use parking_lot::Mutex;

use crate::fastx::{GenericReader, MTGenericReader};
use crate::parallel::processor::GenericProcessor;
use crate::parallel::{error::Result, ProcessError};
use crate::Record;
use std::thread;

use super::{
    InterleavedMultiReader, InterleavedPairedReader, MultiParallelProcessor, MultiReader,
    PairedParallelProcessor, PairedReader, ParallelProcessor,
};

pub struct Wrapper<X>(pub(crate) X);

fn process_sequential_generic<S: MTGenericReader, T>(
    reader: Wrapper<S>,
    processor: &mut T,
) -> Result<()>
where
    T: for<'a> GenericProcessor<S::RefRecord<'a>>,
{
    let mut record_set = reader.0.new_record_set();

    loop {
        match reader.0.fill(&mut record_set).map_err(Into::into)? {
            true => {
                for record in S::iter(&record_set) {
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

fn process_parallel_generic<S: MTGenericReader, T>(
    mut reader: Wrapper<S>,
    processor: &mut T,
    num_threads: usize,
) -> Result<()>
where
    T: for<'a> GenericProcessor<S::RefRecord<'a>>,
{
    if num_threads == 0 {
        return Err(ProcessError::InvalidThreadCount);
    }
    if num_threads == 1 {
        return process_sequential_generic(reader, processor);
    }

    eprintln!("num threads: {num_threads}");

    reader.0.set_num_threads(num_threads);

    thread::scope(|scope| -> Result<()> {
        let reader = &reader;

        // Spawn worker threads
        let mut handles = Vec::new();
        for thread_id in 0..num_threads {
            let mut worker_processor = processor.clone();
            let mut record_set = reader.0.new_record_set();

            let handle = scope.spawn(move || {
                worker_processor.set_thread_id(thread_id);

                loop {
                    let s1 = reader.0.fill(&mut record_set);

                    if !s1.map_err(Into::into)? {
                        break;
                    }

                    let records = S::iter(&record_set);

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

pub trait ParallelReader {
    type Rf<'a>: Record;

    fn process_parallel<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
    where
        T: for<'a> ParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_paired<T>(
        self,
        r2: Self,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_interleaved<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>;

    fn process_parallel_multi<T>(
        self,
        rest: Vec<Self>,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>,
        Self: Sized;

    fn process_parallel_multi_interleaved<T>(
        self,
        arity: usize,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>;
}

impl<S: GenericReader> ParallelReader for S
where
    for<'a> <S as GenericReader>::RefRecord<'a>: Record,
    ProcessError: From<<S as GenericReader>::Error>,
{
    type Rf<'a> = S::RefRecord<'a>;

    fn process_parallel<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
    where
        T: for<'a> ParallelProcessor<S::RefRecord<'a>>,
    {
        process_parallel_generic(Wrapper(Mutex::new(self)), processor, num_threads)
    }

    fn process_parallel_interleaved<T>(self, processor: &mut T, num_threads: usize) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>,
    {
        process_parallel_generic(InterleavedPairedReader::new(self), processor, num_threads)
    }

    fn process_parallel_paired<T>(
        self,
        r2: Self,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> PairedParallelProcessor<Self::Rf<'a>>,
    {
        process_parallel_generic(PairedReader::new(self, r2), processor, num_threads)
    }

    fn process_parallel_multi<T>(
        self,
        mut rest: Vec<Self>,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>,
        Self: Sized,
    {
        rest.insert(0, self);
        process_parallel_generic(MultiReader::new(rest), processor, num_threads)
    }

    fn process_parallel_multi_interleaved<T>(
        self,
        arity: usize,
        processor: &mut T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: for<'a> MultiParallelProcessor<Self::Rf<'a>>,
    {
        process_parallel_generic(
            InterleavedMultiReader::new(self, arity),
            processor,
            num_threads,
        )
    }
}
