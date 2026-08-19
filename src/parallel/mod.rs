mod error;
pub(crate) mod multi;
mod ordered;
pub(crate) mod paired;
#[cfg(feature = "pool")]
pub mod pool;
mod processor;
pub(crate) mod reader;
pub(crate) mod single;

pub use error::{IntoProcessError, ProcessError, Result};
pub use ordered::Ordered;
pub use processor::{MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor};
pub use reader::ParallelReader;

#[cfg(feature = "pool")]
pub use pool::ThreadPool;
#[cfg(feature = "pool")]
pub use reader::PoolParallelReader;
