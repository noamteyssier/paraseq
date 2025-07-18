use super::error::Result;
use crate::Record;

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

/// Trait implemented for a type that processes arbitrary arity (up to 8) record groups in
/// parallel.  The arity here refers to how many synchronized records appear in each group.
/// For example, single-end read files have arity 1. Paired-end read files where the _1 and _2
/// reads are synchronized have arity 2.  Some protocols make use of multiple (>2) input files
/// and may have 3 or more synchronized files.  This trait is used to process the input from
/// those collections of files in an appropriately synchronized way.  The `process_record_multi`
/// function will yield, in turn, a mutable slice to the complete set of records constituting
/// an specific group.
pub trait MultiParallelProcessor: Send + Clone {
    /// Called on a group of records
    fn process_record_multi<Rf: Record>(&mut self, records: &[Rf]) -> Result<()>;

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

/// Trait implemented for a type that processes interleaved record sets in parallel.
/// This processor is designed primarily for interleaved files with record set "arity"
/// greater than 2 (that is, not read pairs, but triplets, quadruplets, etc.).  Though
/// this processor should work fine with pairs, it is recommended instead to use the
/// `InterleavedParallelProcessor` trait for interleaved pairs.
pub trait InterleavedMultiParallelProcessor: Send + Clone {
    /// Called on a pair of records from an interleaved file
    fn process_interleaved_multi<Rf: Record>(&mut self, records: &[Rf]) -> Result<()>;

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
