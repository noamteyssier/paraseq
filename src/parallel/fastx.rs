use std::io::Read;

use crate::{
    fastx,
    parallel::{
        InterleavedMultiParallelProcessor, InterleavedMultiParallelReader,
        InterleavedParallelProcessor, InterleavedParallelReader, PairedParallelProcessor,
        PairedParallelReader, ParallelProcessor, ParallelReader,
    },
    prelude::{MultiParallelProcessor, MultiParallelReader},
};

/// Implements the `ParallelReader` trait for `fastx::Reader`.
///
/// Just matches on internal type and calls the appropriate method.
impl<R: Read + Send> ParallelReader for fastx::Reader<R> {
    fn process_parallel<T>(self, processor: T, num_threads: usize) -> super::Result<()>
    where
        T: ParallelProcessor,
    {
        match self {
            Self::Fasta(reader) => ParallelReader::process_parallel(reader, processor, num_threads),
            Self::Fastq(reader) => reader.process_parallel(processor, num_threads),
        }
    }

    fn process_sequential<T>(self, processor: T) -> super::Result<()>
    where
        T: ParallelProcessor,
    {
        match self {
            Self::Fasta(reader) => reader.process_sequential(processor),
            Self::Fastq(reader) => reader.process_sequential(processor),
        }
    }
}

/// Implements the `PairedParallelReader` trait for `fastx::Reader`.
///
/// Just matches on internal type and calls the appropriate method.
impl<R: Read + Send> PairedParallelReader for fastx::Reader<R> {
    fn process_parallel_paired<T>(
        self,
        reader2: Self,
        processor: T,
        num_threads: usize,
    ) -> super::Result<()>
    where
        T: PairedParallelProcessor,
    {
        match (self, reader2) {
            (Self::Fasta(reader), Self::Fasta(reader2)) => {
                reader.process_parallel_paired(reader2, processor, num_threads)
            }
            (Self::Fastq(reader), Self::Fastq(reader2)) => {
                reader.process_parallel_paired(reader2, processor, num_threads)
            }
            _ => Err(super::ProcessError::IncompatibleReaders),
        }
    }

    fn process_sequential_paired<T>(self, reader2: Self, processor: T) -> super::Result<()>
    where
        T: PairedParallelProcessor,
    {
        match (self, reader2) {
            (Self::Fasta(reader), Self::Fasta(reader2)) => {
                reader.process_sequential_paired(reader2, processor)
            }
            (Self::Fastq(reader), Self::Fastq(reader2)) => {
                reader.process_sequential_paired(reader2, processor)
            }
            _ => Err(super::ProcessError::IncompatibleReaders),
        }
    }
}

/// Implements the `InterleavedParallelReader` trait for `fastx::Reader`.
///
/// Just matches on internal type and calls the appropriate method.
impl<R: Read + Send> InterleavedParallelReader for fastx::Reader<R> {
    fn process_parallel_interleaved<T>(self, processor: T, num_threads: usize) -> super::Result<()>
    where
        T: InterleavedParallelProcessor,
    {
        match self {
            Self::Fasta(reader) => reader.process_parallel_interleaved(processor, num_threads),
            Self::Fastq(reader) => reader.process_parallel_interleaved(processor, num_threads),
        }
    }

    fn process_sequential_interleaved<T>(self, processor: T) -> super::Result<()>
    where
        T: InterleavedParallelProcessor,
    {
        match self {
            Self::Fasta(reader) => reader.process_sequential_interleaved(processor),
            Self::Fastq(reader) => reader.process_sequential_interleaved(processor),
        }
    }
}

impl<R: Read + Send> MultiParallelReader for fastx::Reader<R> {
    fn process_parallel_multi<T>(
        self,
        remaining_readers: Vec<Self>,
        processor: T,
        num_threads: usize,
    ) -> super::Result<()>
    where
        T: MultiParallelProcessor,
    {
        match self {
            Self::Fasta(reader) => {
                let mut readers = vec![];
                remaining_readers
                    .into_iter()
                    .try_for_each(|r| -> Result<(), crate::Error> {
                        readers.push(r.into_fasta_reader()?);
                        Ok(())
                    })?;
                reader.process_parallel_multi(readers, processor, num_threads)
            }
            Self::Fastq(reader) => {
                let mut readers = vec![];
                remaining_readers
                    .into_iter()
                    .try_for_each(|r| -> Result<(), crate::Error> {
                        readers.push(r.into_fastq_reader()?);
                        Ok(())
                    })?;
                reader.process_parallel_multi(readers, processor, num_threads)
            }
        }
    }

    fn process_sequential_multi<T>(
        self,
        remaining_readers: Vec<Self>,
        processor: T,
    ) -> super::Result<()>
    where
        T: MultiParallelProcessor,
    {
        match self {
            Self::Fasta(reader) => {
                let mut readers = vec![];
                remaining_readers
                    .into_iter()
                    .try_for_each(|r| -> Result<(), crate::Error> {
                        readers.push(r.into_fasta_reader()?);
                        Ok(())
                    })?;
                reader.process_sequential_multi(readers, processor)
            }
            Self::Fastq(reader) => {
                let mut readers = vec![];
                remaining_readers
                    .into_iter()
                    .try_for_each(|r| -> Result<(), crate::Error> {
                        readers.push(r.into_fastq_reader()?);
                        Ok(())
                    })?;
                reader.process_sequential_multi(readers, processor)
            }
        }
    }
}

impl<R: Read + Send> InterleavedMultiParallelReader for fastx::Reader<R> {
    fn process_parallel_interleaved_multi<T>(
        self,
        arity: usize,
        processor: T,
        num_threads: usize,
    ) -> super::Result<()>
    where
        T: InterleavedMultiParallelProcessor,
    {
        match self {
            Self::Fasta(reader) => {
                reader.process_parallel_interleaved_multi(arity, processor, num_threads)
            }
            Self::Fastq(reader) => {
                reader.process_parallel_interleaved_multi(arity, processor, num_threads)
            }
        }
    }

    fn process_sequential_interleaved_multi<T>(
        self,
        arity: usize,
        processor: T,
    ) -> super::Result<()>
    where
        T: InterleavedMultiParallelProcessor,
    {
        match self {
            Self::Fasta(reader) => reader.process_sequential_interleaved_multi(arity, processor),
            Self::Fastq(reader) => reader.process_sequential_interleaved_multi(arity, processor),
        }
    }
}
