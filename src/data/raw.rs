use std::path::{Path, PathBuf};

use clam::core::number::Number;

use super::standard::StandardData;

#[derive(Debug, Clone)]
pub struct RawData<T: Number> {
    pub(crate) _t: std::marker::PhantomData<T>,
    pub(crate) base_path: PathBuf,
    pub(crate) query_path: PathBuf,
    pub(crate) ground_path: PathBuf,
    pub(crate) dimensions: usize,
}

impl<T: Number> RawData<T> {
    pub fn from_dir(data_dir: &Path, base_name: &str, query_name: &str, ground_name: &str, dimensions: usize) -> Self {
        assert!(data_dir.exists(), "Path not found: {:?}", data_dir);

        let data = Self {
            base_path: {
                let mut path = data_dir.to_owned();
                path.push("base");
                path.push(base_name);
                path
            },
            query_path: {
                let mut path = data_dir.to_owned();
                path.push("query");
                path.push(query_name);
                path
            },
            ground_path: {
                let mut path = data_dir.to_owned();
                path.push("ground");
                path.push(ground_name);
                path
            },
            dimensions,
            _t: Default::default(),
        };

        assert!(data.base_path.exists(), "Path not found: {:?}", data.base_path);
        assert!(data.query_path.exists(), "Path not found: {:?}", data.query_path);
        assert!(data.ground_path.exists(), "Path not found: {:?}", data.ground_path);

        data
    }

    #[allow(unused, clippy::diverging_sub_expression)]
    pub fn convert(&self, out_dir: &Path, batch_size: usize) -> Result<StandardData<T>, String> {
        let cardinality = todo!();
        let num_queries = todo!();

        let data = StandardData {
            _t: Default::default(),
            data_dir: out_dir.to_owned(),
            dimensionality: self.dimensions,
            cardinality,
            batch_size,
            num_queries,
        };

        Ok(data)
    }
}
