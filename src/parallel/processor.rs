use smallvec::SmallVec;

use crate::{Record, MAX_ARITY};

use super::error::Result;

/// Trait implemented for a type that processes records in parallel
pub trait GenericProcessor<Rf>: Send + Clone {
    /// Called on a batch of records.
    fn process_record_batch(&mut self, records: impl Iterator<Item = Rf>) -> Result<()>;

    /// Called when a batch of records is complete
    fn on_batch_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called when the processing for a thread is complete
    fn on_thread_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Sets the thread id for the processor
    #[allow(unused_variables)]
    fn set_thread_id(&mut self, thread_id: usize) {
        // Default implementation does nothing
    }
}

/// Implement either `process_record_batch` or `process_record`.
/// By default, `process_record_batch` calls `process_record` for each record.
pub trait ParallelProcessor<Rf: Record>: Send + Clone {
    /// Called on a batch of records.
    fn process_record_batch(&mut self, records: impl Iterator<Item = Rf>) -> Result<()> {
        for record in records {
            self.process_record(record)?;
        }
        Ok(())
    }

    /// Called on an individual record
    fn process_record(&mut self, _record: Rf) -> Result<()> {
        unimplemented!("Either ParallelProcessor::process_record or ParallelProcessor::process_record_batch must be implemented!");
    }

    /// Called when a batch of records is complete
    fn on_batch_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called when the processing for a thread is complete
    fn on_thread_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Sets the thread id for the processor
    #[allow(unused_variables)]
    fn set_thread_id(&mut self, thread_id: usize) {
        // Default implementation does nothing
    }

    /// Gets the thread id for the processor
    fn get_thread_id(&self) -> usize {
        unimplemented!("Must be implemented by the processor to be used")
    }
}

impl<Rf: Record, P: ParallelProcessor<Rf>> GenericProcessor<Rf> for P {
    fn process_record_batch(&mut self, records: impl Iterator<Item = Rf>) -> Result<()> {
        self.process_record_batch(records)
    }
    fn on_batch_complete(&mut self) -> Result<()> {
        self.on_batch_complete()
    }
    fn on_thread_complete(&mut self) -> Result<()> {
        self.on_thread_complete()
    }
    fn set_thread_id(&mut self, thread_id: usize) {
        self.set_thread_id(thread_id);
    }
}

pub trait PairedParallelProcessor<Rf: Record>: Send + Clone {
    /// Called on a batch of record pairs.
    fn process_record_pair_batch(
        &mut self,
        record_pairs: impl Iterator<Item = (Rf, Rf)>,
    ) -> Result<()> {
        for record_pair in record_pairs {
            self.process_record_pair(record_pair.0, record_pair.1)?;
        }
        Ok(())
    }

    /// Called on an individual record pair.
    fn process_record_pair(&mut self, _record1: Rf, _record2: Rf) -> Result<()> {
        unimplemented!("Either PairedParallelProcessor::process_record_pair or PairedParallelProcessor::process_record_pair_batch must be implemented!");
    }

    /// Called when a batch of records is complete
    fn on_batch_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called when the processing for a thread is complete
    fn on_thread_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Sets the thread id for the processor
    #[allow(unused_variables)]
    fn set_thread_id(&mut self, thread_id: usize) {
        // Default implementation does nothing
    }

    /// Gets the thread id for the processor
    fn get_thread_id(&self) -> usize {
        unimplemented!("Must be implemented by the processor to be used")
    }
}

impl<Rf: Record, P: PairedParallelProcessor<Rf>> GenericProcessor<(Rf, Rf)> for P {
    fn process_record_batch(&mut self, records: impl Iterator<Item = (Rf, Rf)>) -> Result<()> {
        self.process_record_pair_batch(records)
    }
    fn on_batch_complete(&mut self) -> Result<()> {
        self.on_batch_complete()
    }
    fn on_thread_complete(&mut self) -> Result<()> {
        self.on_thread_complete()
    }
    fn set_thread_id(&mut self, thread_id: usize) {
        self.set_thread_id(thread_id);
    }
}

pub trait MultiParallelProcessor<Rf: Record>: Send + Clone {
    /// Called on a batch of multi-records.
    fn process_multi_record_batch(
        &mut self,
        multi_records: impl Iterator<Item = SmallVec<[Rf; MAX_ARITY]>>,
    ) -> Result<()> {
        for multi_record in multi_records {
            self.process_multi_record(&multi_record)?;
        }
        Ok(())
    }

    /// Called on an individual set of record
    fn process_multi_record(&mut self, _records: &[Rf]) -> Result<()> {
        unimplemented!("Either MultiParallelProcessor::process_multi_record or MultiParallelProcessor::process_multi_record_batch must be implemented!");
    }

    /// Called when a batch of records is complete
    fn on_batch_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called when the processing for a thread is complete
    fn on_thread_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Sets the thread id for the processor
    #[allow(unused_variables)]
    fn set_thread_id(&mut self, thread_id: usize) {
        // Default implementation does nothing
    }

    /// Gets the thread id for the processor
    fn get_thread_id(&self) -> usize {
        unimplemented!("Must be implemented by the processor to be used")
    }
}

impl<Rf: Record, P: MultiParallelProcessor<Rf>> GenericProcessor<SmallVec<[Rf; MAX_ARITY]>> for P {
    fn process_record_batch(
        &mut self,
        multi_records: impl Iterator<Item = SmallVec<[Rf; MAX_ARITY]>>,
    ) -> Result<()> {
        self.process_multi_record_batch(multi_records)
    }
    fn on_batch_complete(&mut self) -> Result<()> {
        self.on_batch_complete()
    }
    fn on_thread_complete(&mut self) -> Result<()> {
        self.on_thread_complete()
    }
    fn set_thread_id(&mut self, thread_id: usize) {
        self.set_thread_id(thread_id);
    }
}
