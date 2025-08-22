use super::error::Result;
use super::processor::GenericProcessor;

pub trait ParallelReader {
    type Rf<'a>;

    fn process_parallel<T>(self, processor: T, num_threads: usize) -> Result<()>
    where
        T: for<'a> GenericProcessor<Self::Rf<'a>>;

    fn process_sequential<T>(self, processor: T) -> Result<()>
    where
        T: for<'a> GenericProcessor<Self::Rf<'a>>;
}
