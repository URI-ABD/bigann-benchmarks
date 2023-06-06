use std::path::PathBuf;

use clam::core::number::Number;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct StandardData<T: Number> {
    pub(crate) _t: std::marker::PhantomData<T>,
    pub(crate) data_dir: PathBuf,
    pub(crate) dimensionality: usize,
    pub(crate) cardinality: usize,
    pub(crate) batch_size: usize,
    pub(crate) num_queries: usize,
}
