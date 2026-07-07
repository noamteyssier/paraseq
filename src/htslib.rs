use std::borrow::Cow;
use std::sync::OnceLock;
use std::{io, path::Path};

use rust_htslib::bam::{self, Read as BamRead};
use thiserror::Error;

use crate::fastx::GenericReader;
use crate::DEFAULT_MAX_RECORDS;
use crate::{
    parallel::{IntoProcessError, Result},
    ProcessError, Record,
};

/// Type alias for the internal reader type used by htslib
pub type HtslibReader = Box<dyn io::Read + Send>;

/// The size of the batch used for parallel processing.
pub const BATCH_SIZE: usize = 1024;

/// Error type for parallel htslib operations.
#[derive(Error, Debug)]
pub enum ParallelHtslibError {
    #[error("Record synchronization error for htslib files.")]
    PairedRecordMismatch,

    #[error("Unpaired record encountered: {0}")]
    UnpairedRecord(String),

    #[error("Paired records with different QNames encountered when expected: {0} != {1}")]
    PairedRecordsWithDifferentQNames(String, String),

    #[error("Paired records with same template position encountered when expected")]
    PairedRecordsWithSameTemplatePosition,
}

pub struct Reader {
    reader: bam::Reader,
    batch_size: Option<usize>,
}
impl Reader {
    pub fn from_optional_path<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let inner = match path {
            Some(path) => bam::Reader::from_path(path)?,
            None => bam::Reader::from_stdin()?,
        };
        Ok(Self {
            reader: inner,
            batch_size: None,
        })
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let inner = bam::Reader::from_path(path)?;
        Ok(Self {
            reader: inner,
            batch_size: None,
        })
    }

    pub fn from_stdin() -> Result<Self> {
        let inner = bam::Reader::from_stdin()?;
        Ok(Self {
            reader: inner,
            batch_size: None,
        })
    }

    pub fn from_reader(reader: bam::Reader) -> Self {
        Self {
            reader,
            batch_size: None,
        }
    }
}

pub struct RefRecord<'a> {
    pub inner: &'a bam::Record,
    /// Used to store ASCII-encoded quality scores as BAM records store raw PHRED scores.
    qual: OnceLock<Option<Vec<u8>>>,
}
impl<'a> RefRecord<'a> {
    pub fn new(record: &'a bam::Record) -> Self {
        Self {
            inner: record,
            qual: OnceLock::new(),
        }
    }
}

impl Record for RefRecord<'_> {
    fn id(&self) -> &[u8] {
        self.inner.qname()
    }
    fn seq(&self) -> Cow<'_, [u8]> {
        self.inner.seq().as_bytes().into()
    }
    fn seq_raw(&self) -> &[u8] {
        unimplemented!("seq_raw is unimplemented by htslib readers")
    }
    fn qual(&self) -> Option<&[u8]> {
        self.qual
            .get_or_init(|| {
                let qual = self.inner.qual();
                if qual.is_empty() {
                    None
                } else {
                    Some(qual.iter().map(|&phred| phred + 33).collect())
                }
            })
            .as_deref()
    }
}

#[derive(Clone)]
pub struct RecordSet {
    records: Vec<bam::Record>,
    n_records: usize,
}
impl Default for RecordSet {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_RECORDS)
    }
}
impl RecordSet {
    pub fn new(capacity: usize) -> Self {
        Self {
            records: vec![bam::Record::default(); capacity],
            n_records: 0,
        }
    }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Result<RefRecord<'_>>> {
        self.records
            .iter()
            .take(self.n_records)
            .map(RefRecord::new)
            .map(Ok)
    }
}

impl GenericReader for Reader {
    type RecordSet = RecordSet;
    type Error = ProcessError;
    type RefRecord<'a> = RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet {
        if let Some(batch_size) = self.batch_size {
            Self::RecordSet::new(batch_size)
        } else {
            Self::RecordSet::new(DEFAULT_MAX_RECORDS)
        }
    }

    fn fill(&mut self, record_set: &mut Self::RecordSet) -> Result<bool> {
        // reset the counter
        record_set.n_records = 0;

        // fill the record set
        for record in &mut record_set.records {
            if let Some(res) = self.reader.read(record) {
                res?;
                record_set.n_records += 1;
            } else {
                break;
            }
        }

        // false if reader is exhausted
        Ok(record_set.n_records > 0)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = Result<Self::RefRecord<'_>>> {
        record_set.iter()
    }

    fn check_read_pair(rec1: &Self::RefRecord<'_>, rec2: &Self::RefRecord<'_>) -> Result<()> {
        let rec1 = rec1.inner;
        let rec2 = rec2.inner;
        if !rec1.is_paired() {
            let qname = std::str::from_utf8(rec1.qname()).unwrap().to_string();
            return Err(ParallelHtslibError::UnpairedRecord(qname).into_process_error());
        }
        if !rec2.is_paired() {
            let qname = std::str::from_utf8(rec2.qname()).unwrap().to_string();
            return Err(ParallelHtslibError::UnpairedRecord(qname).into_process_error());
        }

        if rec1.qname() != rec2.qname() {
            return Err(ParallelHtslibError::PairedRecordsWithDifferentQNames(
                std::str::from_utf8(rec1.qname()).unwrap().to_string(),
                std::str::from_utf8(rec2.qname()).unwrap().to_string(),
            )
            .into_process_error());
        }

        if rec1.is_first_in_template() && rec2.is_first_in_template() {
            return Err(
                ParallelHtslibError::PairedRecordsWithSameTemplatePosition.into_process_error()
            );
        }

        Ok(())
    }

    fn set_threads(&mut self, threads: usize) -> std::result::Result<(), Self::Error> {
        self.reader
            .set_threads(threads)
            .map_err(IntoProcessError::into_process_error)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::parallel::{PairedParallelProcessor, ParallelProcessor, ParallelReader};
    use crate::Record;

    use super::*;

    #[derive(Clone, Default)]
    struct CountingProcessor {
        local_count: usize,
        global_count: Arc<AtomicUsize>,
    }
    impl CountingProcessor {
        fn count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }
    impl<Rf: Record> ParallelProcessor<Rf> for CountingProcessor {
        fn process_record(&mut self, _record: Rf) -> Result<()> {
            self.local_count += 1;
            Ok(())
        }
        fn on_batch_complete(&mut self) -> Result<()> {
            self.global_count
                .fetch_add(self.local_count, Ordering::Relaxed);
            self.local_count = 0;
            Ok(())
        }
    }
    impl<Rf: Record> PairedParallelProcessor<Rf> for CountingProcessor {
        fn process_record_pair(&mut self, _r1: Rf, _r2: Rf) -> Result<()> {
            self.local_count += 1;
            Ok(())
        }
        fn on_batch_complete(&mut self) -> Result<()> {
            self.global_count
                .fetch_add(self.local_count, Ordering::Relaxed);
            self.local_count = 0;
            Ok(())
        }
    }

    #[test]
    fn test_read_single() {
        for ext in ["sam", "bam", "cram"] {
            let path = format!("./data/sample.{ext}");
            dbg!(&path);
            let reader = Reader::from_path(&path).unwrap();
            let mut proc = CountingProcessor::default();
            reader.process_parallel(&mut proc, 1).unwrap();
            assert_eq!(proc.count(), 100);
        }
    }

    #[test]
    fn test_read_paired() {
        for ext in ["sam", "bam", "cram"] {
            let path = format!("./data/paired.{ext}");
            dbg!(&path);
            let reader = Reader::from_path(&path).unwrap();
            let mut proc = CountingProcessor::default();
            reader.process_parallel_interleaved(&mut proc, 1).unwrap();
            assert_eq!(proc.count(), 100);
        }
    }

    #[test]
    fn test_from_reader() {
        let inner = bam::Reader::from_path("./data/sample.sam").unwrap();
        let reader = Reader::from_reader(inner);
        let mut proc = CountingProcessor::default();
        reader.process_parallel(&mut proc, 1).unwrap();
        assert_eq!(proc.count(), 100);
    }

    #[test]
    fn test_from_optional_path_some() {
        let reader = Reader::from_optional_path(Some("./data/sample.sam")).unwrap();
        let mut proc = CountingProcessor::default();
        reader.process_parallel(&mut proc, 1).unwrap();
        assert_eq!(proc.count(), 100);
    }

    #[test]
    fn test_from_stdin() {
        if crate::test_util::is_stdin_child() {
            let reader = Reader::from_optional_path(None::<&std::path::Path>).unwrap();
            let mut proc = CountingProcessor::default();
            reader.process_parallel(&mut proc, 1).unwrap();
            eprintln!("STDIN_COUNT={}", proc.count());
            return;
        }

        let sam_bytes = std::fs::read("./data/sample.sam").unwrap();
        let output =
            crate::test_util::run_with_piped_stdin("htslib::tests::test_from_stdin", &sam_bytes);
        assert!(output.status.success(), "child failed: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("STDIN_COUNT=100"), "stderr: {stderr}");
    }

    fn write_temp_sam(name: &str, body: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        let header = "@HD\tVN:1.6\tSO:unsorted\n";
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "paraseq_htslib_test_{name}_{}_{unique}.sam",
            std::process::id()
        ));
        std::fs::write(&path, format!("{header}{body}")).unwrap();
        path
    }

    // `check_read_pair` is only exercised by the two-reader `process_parallel_paired`
    // path (`PairedReader::iter`) -- the single-reader interleaved path does not
    // validate pairing at all, so these use one crafted record per file.

    #[test]
    fn test_check_read_pair_unpaired_record() {
        // r1 is not flagged as paired (flag=4: unmapped only).
        let path1 = write_temp_sam("unpaired_r1", "readA\t4\t*\t0\t0\t*\t*\t0\t0\tACGT\tIIII\n");
        let path2 = write_temp_sam(
            "unpaired_r2",
            "readA\t69\t*\t0\t0\t*\t*\t0\t0\tACGT\tIIII\n",
        );
        let r1 = Reader::from_path(&path1).unwrap();
        let r2 = Reader::from_path(&path2).unwrap();
        let mut proc = CountingProcessor::default();
        let err = r1.process_parallel_paired(r2, &mut proc, 1).unwrap_err();
        std::fs::remove_file(&path1).ok();
        std::fs::remove_file(&path2).ok();
        assert!(err.to_string().contains("Unpaired record"));
    }

    #[test]
    fn test_check_read_pair_different_qnames() {
        // Both paired and correctly first/second-in-template, but distinct QNAMEs.
        let path1 = write_temp_sam(
            "diff_qname_r1",
            "read1\t77\t*\t0\t0\t*\t*\t0\t0\tACGT\tIIII\n",
        );
        let path2 = write_temp_sam(
            "diff_qname_r2",
            "read2\t141\t*\t0\t0\t*\t*\t0\t0\tACGT\tIIII\n",
        );
        let r1 = Reader::from_path(&path1).unwrap();
        let r2 = Reader::from_path(&path2).unwrap();
        let mut proc = CountingProcessor::default();
        let err = r1.process_parallel_paired(r2, &mut proc, 1).unwrap_err();
        std::fs::remove_file(&path1).ok();
        std::fs::remove_file(&path2).ok();
        assert!(err.to_string().contains("different QNames"));
    }

    #[test]
    fn test_check_read_pair_same_template_position() {
        // Same QNAME, but both flagged as first-in-template (flag bit 64).
        let path1 = write_temp_sam(
            "same_template_r1",
            "readX\t77\t*\t0\t0\t*\t*\t0\t0\tACGT\tIIII\n",
        );
        let path2 = write_temp_sam(
            "same_template_r2",
            "readX\t77\t*\t0\t0\t*\t*\t0\t0\tACGT\tIIII\n",
        );
        let r1 = Reader::from_path(&path1).unwrap();
        let r2 = Reader::from_path(&path2).unwrap();
        let mut proc = CountingProcessor::default();
        let err = r1.process_parallel_paired(r2, &mut proc, 1).unwrap_err();
        std::fs::remove_file(&path1).ok();
        std::fs::remove_file(&path2).ok();
        assert!(err.to_string().contains("same template position"));
    }
}
