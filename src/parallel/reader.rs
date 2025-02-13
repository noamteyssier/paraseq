use std::io;

use super::error::Result;
use super::processor::{PairedParallelProcessor, ParallelProcessor};

pub trait ParallelReader<R>
where
    R: io::Read + Send,
{
    fn process_parallel<T>(self, processor: T, num_threads: usize) -> Result<()>
    where
        T: ParallelProcessor;

    fn process_sequential<T>(self, processor: T) -> Result<()>
    where
        T: ParallelProcessor;
}

/// Trait for parallel processing of paired reads
pub trait PairedParallelReader<R>: ParallelReader<R>
where
    R: io::Read + Send,
{
    /// Process paired FASTQ/FASTA files in parallel
    fn process_parallel_paired<T>(
        self,
        reader2: Self,
        processor: T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: PairedParallelProcessor;

    /// Process paired FASTQ/FASTA files sequentially
    fn process_sequential_paired<T>(self, reader2: Self, processor: T) -> Result<()>
    where
        T: PairedParallelProcessor;
}
