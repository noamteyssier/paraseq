pub(crate) mod multi;
mod ordered;
pub(crate) mod paired;
pub mod pool;
mod pool_worker;
mod processor;
pub(crate) mod reader;
pub(crate) mod single;

pub use ordered::Ordered;
pub use processor::{MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor};
pub use reader::ParallelReader;

pub use pool::ThreadPool;
pub use reader::PoolParallelReader;
