use std::borrow::Cow;
use std::io;

use crate::{fasta, fastq, Error, Record};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Fasta,
    Fastq,
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
    fn seq(&self) -> Cow<[u8]> {
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
    impl ParallelProcessor for Processor {
        fn process_record<Rf: crate::Record>(
            &mut self,
            _record: Rf,
        ) -> crate::parallel::Result<()> {
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
                let proc = Processor::default();
                reader.process_parallel(proc.clone(), 1).unwrap();
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
                let proc = Processor::default();
                reader.process_parallel(proc.clone(), 1).unwrap();
                assert_eq!(proc.n_records(), 100);
            }
        }
    }
}
