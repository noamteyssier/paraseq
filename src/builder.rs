use std::io;
use std::path::{Path, PathBuf};

use crate::{fasta, fastq, fastx, BoxedReader, Error};

enum Source {
    Path(PathBuf),
    Stdin,
    #[cfg(feature = "url")]
    Url(String),
    #[cfg(feature = "ssh")]
    Ssh {
        url: String,
        args: Vec<String>,
    },
    #[cfg(feature = "gcs")]
    Gcs {
        url: String,
        args: Vec<String>,
    },
}

/// Builds a [`BoxedReader`]-backed `fasta`/`fastq`/`fastx` reader from any of
/// the sources this crate knows how to read from (path, stdin, url, ssh, gcs).
pub struct ReaderBuilder {
    source: Source,
    batch_size: Option<usize>,
    record_limit: Option<usize>,
}

impl ReaderBuilder {
    pub fn path<P: AsRef<Path>>(path: P) -> Self {
        Self {
            source: Source::Path(path.as_ref().to_path_buf()),
            batch_size: None,
            record_limit: None,
        }
    }

    pub fn stdin() -> Self {
        Self {
            source: Source::Stdin,
            batch_size: None,
            record_limit: None,
        }
    }

    pub fn optional_path<P: AsRef<Path>>(path: Option<P>) -> Self {
        match path {
            Some(path) => Self::path(path),
            None => Self::stdin(),
        }
    }

    #[cfg(feature = "url")]
    pub fn url(url: &str) -> Self {
        Self {
            source: Source::Url(url.to_string()),
            batch_size: None,
            record_limit: None,
        }
    }

    #[cfg(feature = "ssh")]
    pub fn ssh(url: &str) -> Self {
        Self {
            source: Source::Ssh {
                url: url.to_string(),
                args: Vec::new(),
            },
            batch_size: None,
            record_limit: None,
        }
    }

    #[cfg(feature = "gcs")]
    pub fn gcs(url: &str) -> Self {
        Self {
            source: Source::Gcs {
                url: url.to_string(),
                args: Vec::new(),
            },
            batch_size: None,
            record_limit: None,
        }
    }

    #[must_use]
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = Some(n);
        self
    }

    #[must_use]
    pub fn record_limit(mut self, n: usize) -> Self {
        self.record_limit = Some(n);
        self
    }

    #[cfg(feature = "ssh")]
    #[must_use]
    pub fn ssh_args(mut self, args: &[&str]) -> Self {
        if let Source::Ssh { args: dst, .. } = &mut self.source {
            dst.extend(args.iter().map(|s| s.to_string()));
        }
        self
    }

    #[cfg(feature = "gcs")]
    #[must_use]
    pub fn gcloud_args(mut self, args: &[&str]) -> Self {
        if let Source::Gcs { args: dst, .. } = &mut self.source {
            dst.extend(args.iter().map(|s| s.to_string()));
        }
        self
    }

    #[cfg(feature = "gcs")]
    #[must_use]
    pub fn project(self, project_id: &str) -> Self {
        self.gcloud_args(&["--project", project_id])
    }

    fn open(self) -> Result<(BoxedReader, Option<usize>, Option<usize>), Error> {
        let reader: BoxedReader = match self.source {
            Source::Path(path) => {
                let (reader, _format) = niffler::send::from_path(path)?;
                reader
            }
            Source::Stdin => {
                let (reader, _format) = niffler::send::get_reader(Box::new(io::stdin()))?;
                reader
            }
            #[cfg(feature = "url")]
            Source::Url(url) => {
                let stream = reqwest::blocking::get(&url)?;
                let (reader, _format) = niffler::send::get_reader(Box::new(stream))?;
                reader
            }
            #[cfg(feature = "ssh")]
            Source::Ssh { url, args } => {
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                let ssh_reader = crate::ssh::SshReader::with_ssh_args(&url, &args)?;
                let (reader, _format) = niffler::send::get_reader(Box::new(ssh_reader))?;
                reader
            }
            #[cfg(feature = "gcs")]
            Source::Gcs { url, args } => {
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                let gcs_reader = crate::gcs::GcsReader::with_gcloud_args(&url, &args)?;
                let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
                reader
            }
        };
        Ok((reader, self.batch_size, self.record_limit))
    }

    pub fn build_fasta(self) -> Result<fasta::Reader<BoxedReader>, Error> {
        let (reader, batch_size, record_limit) = self.open()?;
        let mut reader = fasta::Reader::new(reader);
        if let Some(n) = batch_size {
            reader.set_batch_size(n)?;
        }
        if let Some(n) = record_limit {
            reader.set_record_limit(n);
        }
        Ok(reader)
    }

    pub fn build_fastq(self) -> Result<fastq::Reader<BoxedReader>, Error> {
        let (reader, batch_size, record_limit) = self.open()?;
        let mut reader = fastq::Reader::new(reader);
        if let Some(n) = batch_size {
            reader.set_batch_size(n)?;
        }
        if let Some(n) = record_limit {
            reader.set_record_limit(n);
        }
        Ok(reader)
    }

    pub fn build(self) -> Result<fastx::Reader<BoxedReader>, Error> {
        let (reader, batch_size, record_limit) = self.open()?;
        let mut reader = fastx::Reader::new(reader)?;
        if let Some(n) = batch_size {
            reader.set_batch_size(n)?;
        }
        if let Some(n) = record_limit {
            reader.set_record_limit(n);
        }
        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_fasta() {
        let mut reader = ReaderBuilder::path("./data/sample.fasta")
            .batch_size(2)
            .build_fasta()
            .unwrap();
        let mut rset = reader.new_record_set();
        assert!(rset.fill(&mut reader).unwrap());
    }

    #[test]
    fn test_build_fastq() {
        let mut reader = ReaderBuilder::path("./data/sample.fastq")
            .batch_size(2)
            .build_fastq()
            .unwrap();
        let mut rset = reader.new_record_set();
        assert!(rset.fill(&mut reader).unwrap());
    }

    #[test]
    fn test_build_fastx() {
        let mut reader = ReaderBuilder::path("./data/sample.fastq")
            .batch_size(2)
            .build()
            .unwrap();
        let mut rset = reader.new_record_set();
        assert!(rset.fill(&mut reader).unwrap());
    }

    #[cfg(feature = "gcs")]
    #[test]
    fn test_gcloud_args_chaining() {
        let builder = ReaderBuilder::gcs("gs://bucket/object")
            .project("my-project")
            .gcloud_args(&["--billing-project", "billing"]);
        match builder.source {
            Source::Gcs { args, .. } => {
                assert_eq!(
                    args,
                    vec!["--project", "my-project", "--billing-project", "billing"]
                );
            }
            _ => panic!("expected gcs source"),
        }
    }
}
