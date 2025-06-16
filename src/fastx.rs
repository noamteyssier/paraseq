use std::io::Read;

use super::{fasta, fastq, Error};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Fasta,
    Fastq,
}

pub enum Reader<R: Read> {
    Fasta(fasta::Reader<R>),
    Fastq(fastq::Reader<R>),
}
impl<R: Read> Reader<R> {
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

    pub fn format(&self) -> Format {
        match self {
            Self::Fasta(_) => Format::Fasta,
            Self::Fastq(_) => Format::Fastq,
        }
    }
}
