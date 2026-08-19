pub use crate::{
    parallel::{
        IntoProcessError, MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor,
        ParallelReader,
    },
    Record,
};

#[cfg(feature = "pool")]
pub use crate::parallel::{PoolParallelReader, ThreadPool};
