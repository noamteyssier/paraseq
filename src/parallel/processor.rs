use super::error::Result;
use crate::fastx::Record;

/// Trait implemented for a type that processes records in parallel
pub trait ParallelProcessor: Send + Clone {
    /// Called on an individual record
    fn process_record<Rf: Record>(&mut self, record: Rf) -> Result<()>;

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

/// Trait implemented for a type that processes pairs of records in parallel
pub trait PairedParallelProcessor: Send + Clone {
    /// Called on a pair of records
    fn process_record_pair<Rf: Record>(&mut self, record1: Rf, record2: Rf) -> Result<()>;

    /// Called when a batch of pairs is complete
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

/// Trait implemented for a type that processes interleaved records in parallel
pub trait InterleavedParallelProcessor: Send + Clone {
    /// Called on a pair of records from an interleaved file
    fn process_interleaved_pair<Rf: Record>(&mut self, record1: Rf, record2: Rf) -> Result<()>;

    /// Called when a batch of interleaved pairs is complete
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
