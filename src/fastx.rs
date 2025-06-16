use std::io;

use super::{fasta, fastq, Error};

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
