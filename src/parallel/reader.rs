use std::io;

use super::error::Result;
use super::processor::{
    InterleavedParallelMultiProcessor, InterleavedParallelProcessor, MultiParallelProcessor,
    PairedParallelProcessor, ParallelProcessor,
};

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

/// Trait for parallel processing of fixed-arity read sets
pub trait MultiParallelReader<R>: ParallelReader<R>
where
    R: io::Read + Send,
{
    /// Process paired FASTQ/FASTA files in parallel
    fn process_parallel_multi<T>(
        self,
        remaining_readers: &mut [Self],
        processor: T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: MultiParallelProcessor,
        Self: std::marker::Sized;

    /// Process paired FASTQ/FASTA files sequentially
    fn process_sequential_multi<T>(
        self,
        remaining_readers: &mut [Self],
        processor: T,
    ) -> Result<()>
    where
        T: MultiParallelProcessor,
        Self: std::marker::Sized;
}

/// Trait for parallel processing of interleaved reads
pub trait InterleavedParallelReader<R>: ParallelReader<R>
where
    R: io::Read + Send,
{
    /// Process interleaved FASTQ/FASTA files in parallel
    fn process_parallel_interleaved<T>(self, processor: T, num_threads: usize) -> Result<()>
    where
        T: InterleavedParallelProcessor;

    /// Process interleaved FASTQ/FASTA files sequentially
    fn process_sequential_interleaved<T>(self, processor: T) -> Result<()>
    where
        T: InterleavedParallelProcessor;
}

/// Trait for parallel processing of interleaved reads
pub trait InterleavedParallelMultiReader<R>: ParallelReader<R>
where
    R: io::Read + Send,
{
    /// Process interleaved FASTQ/FASTA files in parallel
    fn process_parallel_interleaved_multi<T>(
        self,
        arity: usize,
        processor: T,
        num_threads: usize,
    ) -> Result<()>
    where
        T: InterleavedParallelMultiProcessor;

    /// Process interleaved FASTQ/FASTA files sequentially
    fn process_sequential_interleaved_multi<T>(self, arity: usize, processor: T) -> Result<()>
    where
        T: InterleavedParallelMultiProcessor;
}
