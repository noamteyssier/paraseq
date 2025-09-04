use smallvec::SmallVec;

use crate::{Record, MAX_ARITY};

use super::error::Result;

/// Trait implemented for a type that processes records in parallel
pub trait GenericProcessor<Rf>: Send + Clone {
    /// Called on an individual record
    fn process_record(&mut self, record: Rf) -> Result<()>;

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

pub trait ParallelProcessor<Rf: Record>: Send + Clone {
    /// Called on an individual record
    fn process_record(&mut self, record: Rf) -> Result<()>;

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
    fn process_record(&mut self, record: Rf) -> Result<()> {
        self.process_record(record)
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
    /// Called on an individual record
    fn process_record_pair(&mut self, record1: Rf, record2: Rf) -> Result<()>;

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
    fn process_record(&mut self, record: (Rf, Rf)) -> Result<()> {
        self.process_record_pair(record.0, record.1)
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
    /// Called on an individual record
    fn process_multi_record(&mut self, records: &[Rf]) -> Result<()>;

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
    fn process_record(&mut self, record: SmallVec<[Rf; MAX_ARITY]>) -> Result<()> {
        self.process_multi_record(&record)
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
