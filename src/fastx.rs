use std::io;
use std::path::Path;
use std::{borrow::Cow, thread};

use log::warn;

use crate::parallel::multi::{InterleavedMultiReader, MultiReader};
use crate::parallel::paired::{InterleavedPairedReader, PairedReader};
use crate::ProcessError;
use crate::{
    fasta, fastq,
    parallel::single::{process_parallel_generic, SingleReader},
    Error, Record,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Fasta,
    Fastq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectionType {
    Single,
    Paired,
    Interleaved,
    Multi { arity: usize },
    InterleavedMulti { arity: usize },
}

/// A reader over multiple `fastx::Reader`s.
pub struct Collection<R: io::Read> {
    inner: Vec<Reader<R>>,
    collection_type: CollectionType,
}
impl<R: io::Read> Collection<R> {
    pub fn new(inner: Vec<Reader<R>>, collection_type: CollectionType) -> crate::Result<Self> {
        let reader = Self {
            inner,
            collection_type,
        };
        reader.validate_arity()?;
        Ok(reader)
    }

    fn validate_arity(&self) -> crate::Result<()> {
        match self.collection_type {
            CollectionType::Paired => {
                if !self.inner.len().is_multiple_of(2) {
                    return Err(ProcessError::CollectionSizeMismatch {
                        arity: 2,
                        found: self.inner.len(),
                    });
                }
            }
            CollectionType::Multi { arity } => {
                if !self.inner.len().is_multiple_of(arity) {
                    return Err(ProcessError::CollectionSizeMismatch {
                        arity,
                        found: self.inner.len(),
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }
}
impl Collection<Box<dyn io::Read + Send>> {
    pub fn from_paths<P: AsRef<Path>>(
        paths: &[P],
        collection_type: CollectionType,
    ) -> crate::Result<Self> {
        let mut inner = Vec::new();
        for path in paths {
            inner.push(Reader::from_path(path)?);
        }
        Ok(Self::new(inner, collection_type)?)
    }
}

impl<R: io::Read + Send> Collection<R> {
    /// Generic handler for single-reader-per-thread pattern
    fn handle_single_readers<T, F>(
        self,
        processor: &mut T,
        num_threads: usize,
        scope_fn: F,
    ) -> crate::Result<()>
    where
        T: Clone + Send,
        F: Fn(Reader<R>, &mut T, usize) -> crate::Result<()> + Send + Sync,
    {
        thread::scope(|scope| -> crate::Result<()> {
            let scope_fn = &scope_fn;
            let handles: Vec<_> = self
                .inner
                .into_iter()
                .map(|reader| {
                    let mut thread_proc = processor.clone();
                    scope.spawn(move || scope_fn(reader, &mut thread_proc, num_threads))
                })
                .collect();

            for handle in handles {
                handle
                    .join()
                    .map_err(|_| crate::ProcessError::JoinError)??;
            }

            Ok(())
        })
    }

    /// Generic handler for arity-based (grouped readers) pattern
    fn handle_grouped_readers<T, F>(
        mut self,
        processor: &mut T,
        num_threads: usize,
        arity: usize,
        scope_fn: F,
    ) -> crate::Result<()>
    where
        T: Clone + Send,
        F: Fn(Vec<Reader<R>>, &mut T, usize) -> crate::Result<()> + Send + Sync,
    {
        thread::scope(|scope| -> crate::Result<()> {
            let scope_fn = &scope_fn;
            let mut handles = Vec::new();
            let total_groups = self.inner.len() / arity;

            for _ in 0..total_groups {
                let mut thread_proc = processor.clone();
                let group: Vec<_> = self.inner.drain(..arity).collect();
                handles.push(scope.spawn(move || scope_fn(group, &mut thread_proc, num_threads)));
            }

            for handle in handles {
                handle
                    .join()
                    .map_err(|_| crate::ProcessError::JoinError)??;
            }

            Ok(())
        })
    }

    pub fn process_parallel<T>(self, processor: &mut T, num_threads: usize) -> crate::Result<()>
    where
        T: for<'a> crate::prelude::ParallelProcessor<RefRecord<'a>>,
    {
        match self.collection_type {
            CollectionType::Single => {}
            CollectionType::Paired => {
                warn!("Processing paired reads as single reads")
            }
            CollectionType::Interleaved => {
                warn!("Processing interleaved reads as single reads")
            }
            CollectionType::Multi { arity } => {
                warn!("Processing multi reads (arity: {arity}) as single reads");
            }
            CollectionType::InterleavedMulti { arity } => {
                warn!("Processing interleaved multi reads (arity: {arity}) as single reads");
            }
        }

        self.handle_single_readers(processor, num_threads, |reader, proc, threads| {
            process_parallel_generic(SingleReader::new(reader), proc, threads)
        })
    }

    pub fn process_parallel_paired<T>(
        self,
        processor: &mut T,
        num_threads: usize,
    ) -> crate::Result<()>
    where
        T: for<'a> crate::prelude::PairedParallelProcessor<RefRecord<'a>>,
    {
        match self.collection_type {
            CollectionType::Single => {
                warn!("Processing single reads as paired reads")
            }
            CollectionType::Paired => {}
            CollectionType::Interleaved => {
                warn!("Processing interleaved reads as paired reads")
            }
            CollectionType::Multi { arity } => {
                if arity != 2 {
                    warn!("Processing multi reads (arity: {arity}) as paired reads");
                }
            }
            CollectionType::InterleavedMulti { arity } => {
                if arity != 2 {
                    warn!("Processing interleaved multi reads (arity: {arity}) as paired reads");
                }
            }
        }

        self.handle_grouped_readers(processor, num_threads, 2, |mut readers, proc, threads| {
            let r1 = readers.remove(0);
            let r2 = readers.remove(0);
            process_parallel_generic(PairedReader::new(r1, r2), proc, threads)
        })
    }

    pub fn process_parallel_interleaved<T>(
        self,
        processor: &mut T,
        num_threads: usize,
    ) -> crate::Result<()>
    where
        T: for<'a> crate::prelude::PairedParallelProcessor<RefRecord<'a>>,
    {
        match self.collection_type {
            CollectionType::Single => {
                warn!("Processing single reads as interleaved reads")
            }
            CollectionType::Paired => {
                warn!("Processing paired reads as interleaved reads")
            }
            CollectionType::Interleaved => {}
            CollectionType::Multi { arity } => {
                warn!("Processing multi reads (arity: {arity}) as interleaved reads");
            }
            CollectionType::InterleavedMulti { arity } => {
                if arity != 2 {
                    warn!(
                        "Processing interleaved multi reads (arity: {arity}) as interleaved reads"
                    );
                }
            }
        }

        self.handle_single_readers(processor, num_threads, |reader, proc, threads| {
            process_parallel_generic(InterleavedPairedReader::new(reader), proc, threads)
        })
    }

    pub fn process_parallel_multi<T>(
        self,
        processor: &mut T,
        num_threads: usize,
    ) -> crate::Result<()>
    where
        T: for<'a> crate::prelude::MultiParallelProcessor<RefRecord<'a>>,
        Self: Sized,
    {
        let arity = match self.collection_type {
            CollectionType::Single => {
                warn!("Processing single reads as multi-reads (arity=1)");
                1
            }
            CollectionType::Paired => {
                warn!("Processing paired reads as multi-reads (arity=2)");
                2
            }
            CollectionType::Interleaved => {
                warn!("Processing interleaved reads as multi-reads (arity=1)");
                1
            }
            CollectionType::Multi { arity } => arity,
            CollectionType::InterleavedMulti { arity } => {
                if arity != 2 {
                    warn!(
                        "Processing interleaved multi reads (arity: {arity}) as multi-reads (arity={arity})"
                    );
                }
                arity
            }
        };

        self.handle_grouped_readers(
            processor,
            num_threads,
            arity,
            |mut readers, proc, threads| {
                let mut rest = Vec::new();
                rest.extend(readers.drain(..arity));
                process_parallel_generic(MultiReader::new(rest), proc, threads)
            },
        )
    }

    pub fn process_parallel_multi_interleaved<T>(
        self,
        processor: &mut T,
        num_threads: usize,
    ) -> crate::Result<()>
    where
        T: for<'a> crate::prelude::MultiParallelProcessor<RefRecord<'a>>,
    {
        let arity = match self.collection_type {
            CollectionType::Single => {
                warn!("Processing single reads as interleaved multi reads (arity: 1)");
                1
            }
            CollectionType::Paired => {
                warn!("Processing paired reads as interleaved multi reads (arity: 2)");
                2
            }
            CollectionType::Interleaved => {
                warn!("Processing interleaved reads as interleaved multi reads (arity: 2)");
                2
            }
            CollectionType::Multi { arity } => {
                warn!("Processing multi reads (arity: {arity}) as interleaved multi reads");
                arity
            }
            CollectionType::InterleavedMulti { arity } => arity,
        };

        self.handle_single_readers(processor, num_threads, |reader, proc, threads| {
            process_parallel_generic(InterleavedMultiReader::new(reader, arity), proc, threads)
        })
    }
}

pub enum Reader<R: io::Read> {
    Fasta(fasta::Reader<R>),
    Fastq(fastq::Reader<R>),
}

#[cfg(feature = "niffler")]
impl Reader<Box<dyn io::Read + Send>> {
    pub fn from_path<P: AsRef<std::path::Path>>(path: P) -> Result<Self, Error> {
        let (reader, _format) = niffler::send::from_path(path)?;
        Self::new(reader)
    }

    pub fn from_stdin() -> Result<Self, Error> {
        let (reader, _format) = niffler::send::get_reader(Box::new(io::stdin()))?;
        Self::new(reader)
    }

    pub fn from_optional_path<P: AsRef<std::path::Path>>(path: Option<P>) -> Result<Self, Error> {
        match path {
            Some(path) => Self::from_path(path),
            None => Self::from_stdin(),
        }
    }

    pub fn from_path_with_batch_size<P: AsRef<std::path::Path>>(
        path: P,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let (reader, _format) = niffler::send::from_path(path)?;
        Self::new_with_batch_size(reader, batch_size)
    }

    pub fn from_stdin_with_batch_size(batch_size: usize) -> Result<Self, Error> {
        let (reader, _format) = niffler::send::get_reader(Box::new(io::stdin()))?;
        Self::new_with_batch_size(reader, batch_size)
    }

    pub fn from_optional_path_with_batch_size<P: AsRef<std::path::Path>>(
        path: Option<P>,
        batch_size: usize,
    ) -> Result<Self, Error> {
        match path {
            Some(path) => Self::from_path_with_batch_size(path, batch_size),
            None => Self::from_stdin_with_batch_size(batch_size),
        }
    }
}

#[cfg(feature = "url")]
impl Reader<Box<dyn io::Read + Send>> {
    pub fn from_url(url: &str) -> Result<Self, Error> {
        let stream = reqwest::blocking::get(url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(stream))?;
        Self::new(reader)
    }

    pub fn from_url_with_batch_size(url: &str, batch_size: usize) -> Result<Self, Error> {
        let stream = reqwest::blocking::get(url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(stream))?;
        Self::new_with_batch_size(reader, batch_size)
    }
}

#[cfg(feature = "ssh")]
impl Reader<Box<dyn io::Read + Send>> {
    pub fn from_ssh(ssh_url: &str) -> Result<Self, Error> {
        let ssh_reader = crate::ssh::SshReader::new(ssh_url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(ssh_reader))?;
        Self::new(reader)
    }

    pub fn from_ssh_with_batch_size(ssh_url: &str, batch_size: usize) -> Result<Self, Error> {
        let ssh_reader = crate::ssh::SshReader::new(ssh_url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(ssh_reader))?;
        Self::new_with_batch_size(reader, batch_size)
    }
}

#[cfg(feature = "gcs")]
impl Reader<Box<dyn io::Read + Send>> {
    /// Create a GCS reader using Application Default Credentials
    pub fn from_gcs(gcs_url: &str) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::new(gcs_url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Self::new(reader)
    }

    /// Create a GCS reader using custom gcloud arguments
    pub fn from_gcs_with_gcloud_args(gcs_url: &str, args: &[&str]) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::with_gcloud_args(gcs_url, args)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Self::new(reader)
    }

    /// Create a GCS reader using a specific project ID
    pub fn from_gcs_with_project(gcs_url: &str, project_id: &str) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::with_project(gcs_url, project_id)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Self::new(reader)
    }

    /// Create a GCS reader with custom batch size using Application Default Credentials
    pub fn from_gcs_with_batch_size(gcs_url: &str, batch_size: usize) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::new(gcs_url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Self::new_with_batch_size(reader, batch_size)
    }

    /// Create a GCS reader with custom batch size using custom gcloud arguments
    pub fn from_gcs_with_gcloud_args_and_batch_size(
        gcs_url: &str,
        gcloud_args: &[&str],
        batch_size: usize,
    ) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::with_gcloud_args(gcs_url, gcloud_args)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Self::new_with_batch_size(reader, batch_size)
    }

    /// Create a GCS reader with custom batch size using a specific project ID
    pub fn from_gcs_with_project_and_batch_size(
        gcs_url: &str,
        project_id: &str,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::with_project(gcs_url, project_id)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Self::new_with_batch_size(reader, batch_size)
    }
}

impl<R: io::Read> Reader<R> {
    pub fn new(mut reader: R) -> Result<Self, Error> {
        let mut buffer = [0; 1];
        reader.read_exact(&mut buffer)?;
        match buffer {
            [b'@'] => {
                let mut rdr = fastq::Reader::new(reader);
                rdr.add_to_overflow(&buffer);
                Ok(Self::Fastq(rdr))
            }
            [b'>'] => {
                let mut rdr = fasta::Reader::new(reader);
                rdr.add_to_overflow(&buffer);
                Ok(Self::Fasta(rdr))
            }
            _ => Err(Error::InvalidStartCharacter(buffer[0].into())),
        }
    }

    pub fn new_with_batch_size(mut reader: R, batch_size: usize) -> Result<Self, Error> {
        let mut buffer = [0; 1];
        reader.read_exact(&mut buffer)?;
        match buffer {
            [b'@'] => {
                let mut rdr = fastq::Reader::with_batch_size(reader, batch_size)?;
                rdr.add_to_overflow(&buffer);
                Ok(Self::Fastq(rdr))
            }
            [b'>'] => {
                let mut rdr = fasta::Reader::with_batch_size(reader, batch_size)?;
                rdr.add_to_overflow(&buffer);
                Ok(Self::Fasta(rdr))
            }
            _ => Err(Error::InvalidStartCharacter(buffer[0].into())),
        }
    }

    /// Use the first record in the input to set the number of records per batch
    /// so that the expected length per batch is approximately `batch_size_in_bp`.
    pub fn update_batch_size_in_bp(&mut self, batch_size_in_bp: usize) -> Result<(), Error> {
        match self {
            Self::Fasta(inner) => inner.update_batch_size_in_bp(batch_size_in_bp),
            Self::Fastq(inner) => inner.update_batch_size_in_bp(batch_size_in_bp),
        }
    }

    pub fn format(&self) -> Format {
        match self {
            Self::Fasta(_) => Format::Fasta,
            Self::Fastq(_) => Format::Fastq,
        }
    }

    pub fn new_record_set(&self) -> RecordSet {
        match self {
            Self::Fasta(inner) => RecordSet::Fasta(inner.new_record_set()),
            Self::Fastq(inner) => RecordSet::Fastq(inner.new_record_set()),
        }
    }

    pub fn new_record_set_with_size(&self, size: usize) -> RecordSet {
        match self {
            Self::Fasta(inner) => RecordSet::Fasta(inner.new_record_set_with_size(size)),
            Self::Fastq(inner) => RecordSet::Fastq(inner.new_record_set_with_size(size)),
        }
    }

    pub fn reload(&mut self, rset: &mut RecordSet) -> Result<(), Error> {
        match (self, rset) {
            (Self::Fasta(inner), RecordSet::Fasta(rset)) => {
                inner.reload(rset);
                Ok(())
            }
            (Self::Fastq(inner), RecordSet::Fastq(rset)) => {
                inner.reload(rset);
                Ok(())
            }
            _ => Err(Error::FormatMismatch),
        }
    }

    pub fn into_fasta_reader(self) -> Result<fasta::Reader<R>, Error> {
        match self {
            Self::Fasta(inner) => Ok(inner),
            _ => Err(Error::UnexpectedFormatRequest(
                "FASTQ".to_string(),
                "FASTA".to_string(),
            )),
        }
    }

    pub fn into_fastq_reader(self) -> Result<fastq::Reader<R>, Error> {
        match self {
            Self::Fastq(inner) => Ok(inner),
            _ => Err(Error::UnexpectedFormatRequest(
                "FASTA".to_string(),
                "FASTQ".to_string(),
            )),
        }
    }
}

pub enum RecordSet {
    Fasta(fasta::RecordSet),
    Fastq(fastq::RecordSet),
}
impl RecordSet {
    pub fn fill<R: io::Read>(&mut self, reader: &mut Reader<R>) -> Result<bool, Error> {
        match (self, reader) {
            (RecordSet::Fasta(records), Reader::Fasta(reader)) => records.fill(reader),
            (RecordSet::Fastq(records), Reader::Fastq(reader)) => records.fill(reader),
            _ => Err(Error::FormatMismatch),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = Result<RefRecord<'_>, Error>> + '_> {
        match self {
            RecordSet::Fasta(records) => Box::new(
                records
                    .iter()
                    .map(|record| -> Result<RefRecord<'_>, Error> {
                        Ok(RefRecord::Fasta(record?))
                    }),
            ),
            RecordSet::Fastq(records) => Box::new(
                records
                    .iter()
                    .map(|record| -> Result<RefRecord<'_>, Error> {
                        Ok(RefRecord::Fastq(record?))
                    }),
            ),
        }
    }
}

pub enum RefRecord<'a> {
    Fasta(fasta::RefRecord<'a>),
    Fastq(fastq::RefRecord<'a>),
}

impl Record for RefRecord<'_> {
    fn id(&self) -> &[u8] {
        match self {
            Self::Fasta(x) => x.id(),
            Self::Fastq(x) => x.id(),
        }
    }
    fn seq(&self) -> Cow<'_, [u8]> {
        match self {
            Self::Fasta(x) => x.seq(),
            Self::Fastq(x) => x.seq(),
        }
    }
    fn seq_raw(&self) -> &[u8] {
        match self {
            Self::Fasta(x) => x.seq_raw(),
            Self::Fastq(x) => x.seq_raw(),
        }
    }
    fn qual(&self) -> Option<&[u8]> {
        match self {
            Self::Fasta(x) => x.qual(),
            Self::Fastq(x) => x.qual(),
        }
    }
}

/// An internal trait implemented both by fasta and fastq types,
/// so we only have to write the reader and parallel IO implementations once.
pub trait GenericReader: Send {
    type RecordSet: Send + 'static;
    type Error;
    type RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet;
    fn fill(&mut self, record: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error>;
    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>>;
    fn check_read_pair(
        _rec1: &Self::RefRecord<'_>,
        _rec2: &Self::RefRecord<'_>,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

impl<R> GenericReader for crate::fastx::Reader<R>
where
    R: io::Read + Send,
{
    type RecordSet = RecordSet;
    type Error = Error;
    type RefRecord<'a> = crate::fastx::RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet {
        match self {
            Reader::Fasta(inner) => RecordSet::Fasta(inner.new_record_set()),
            Reader::Fastq(inner) => RecordSet::Fastq(inner.new_record_set()),
        }
    }

    fn fill(&mut self, record: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        record.fill(self)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
        match record_set {
            RecordSet::Fasta(record_set) => either::Either::Left(
                fasta::Reader::<R>::iter(record_set).map(|x| x.map(RefRecord::Fasta)),
            ),
            RecordSet::Fastq(record_set) => either::Either::Right(
                fastq::Reader::<R>::iter(record_set).map(|x| x.map(RefRecord::Fastq)),
            ),
        }
    }
}

#[cfg(feature = "niffler")]
#[cfg(test)]
mod testing {

    use crate::prelude::{ParallelProcessor, ParallelReader};
    use parking_lot::Mutex;
    use std::sync::Arc;

    use super::*;

    const FORMAT_EXTENSIONS: &[&str] = &[".fasta", ".fastq"];
    const COMPRESSION_EXTENSIONS: &[&str] = &["", ".gz", ".zst"];

    #[derive(Clone, Default)]
    struct Processor {
        local_count: usize,
        global_count: Arc<Mutex<usize>>,
    }
    impl Processor {
        pub fn n_records(&self) -> usize {
            *self.global_count.lock()
        }
    }
    impl<Rf: crate::Record> ParallelProcessor<Rf> for Processor {
        fn process_record(&mut self, _record: Rf) -> crate::parallel::Result<()> {
            self.local_count += 1;
            Ok(())
        }
        fn on_batch_complete(&mut self) -> crate::parallel::Result<()> {
            *self.global_count.lock() += self.local_count;
            self.local_count = 0;
            Ok(())
        }
    }

    #[test]
    fn test_fastx_reader_from_path() {
        let basename = "./data/sample";
        for format_ext in FORMAT_EXTENSIONS {
            for compression_ext in COMPRESSION_EXTENSIONS {
                let path = format!("{}{}{}", basename, format_ext, compression_ext);
                dbg!(&path);
                let reader = Reader::from_path(path).unwrap();
                let mut proc = Processor::default();
                reader.process_parallel(&mut proc, 1).unwrap();
                assert_eq!(proc.n_records(), 100);
            }
        }
    }

    #[test]
    fn test_fastx_reader_from_path_with_batch_size() {
        let basename = "./data/sample";
        for format_ext in FORMAT_EXTENSIONS {
            for compression_ext in COMPRESSION_EXTENSIONS {
                let path = format!("{}{}{}", basename, format_ext, compression_ext);
                dbg!(&path);
                let reader = Reader::from_path_with_batch_size(path, 10).unwrap();
                let mut proc = Processor::default();
                reader.process_parallel(&mut proc, 1).unwrap();
                assert_eq!(proc.n_records(), 100);
            }
        }
    }
}
