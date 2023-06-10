use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use clam::number::Number;
use ndarray::prelude::*;
use std::io::Read;
use std::path::{Path, PathBuf};

use super::number_to_arrow::IntoArrowArray;
use super::standard::StandardData;

#[derive(Debug, Clone)]
pub(crate) struct RawData<T: Number> {
    pub _t: std::marker::PhantomData<T>,
    pub base_path: PathBuf,
    pub query_path: PathBuf,
    pub ground_path: PathBuf,
}

impl<T: IntoArrowArray> RawData<T> {
    pub fn from_dir(data_dir: &Path, base_name: &str, query_name: &str, ground_name: &str) -> Self {
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
            _t: Default::default(),
        };

        assert!(data.base_path.exists(), "Path not found: {:?}", data.base_path);
        assert!(data.query_path.exists(), "Path not found: {:?}", data.query_path);
        assert!(data.ground_path.exists(), "Path not found: {:?}", data.ground_path);

        data
    }

    pub fn convert(&self, out_dir: &Path, batch_size: usize) -> Result<StandardData<T>, String> {
        let [cardinality, dimensionality] = convert_vectors::<T>(&self.base_path, batch_size, "base", out_dir)?;
        let [num_queries, _] = convert_vectors::<T>(&self.query_path, batch_size, "query", out_dir)?;

        let data = StandardData {
            _t: Default::default(),
            data_dir: out_dir.to_owned(),
            dimensionality,
            cardinality,
            batch_size,
            num_queries,
        };

        Ok(data)
    }
}

fn convert_vectors<T: IntoArrowArray>(
    inp_path: &Path,
    batch_size: usize,
    name: &str,
    out_dir: &Path,
) -> Result<[usize; 2], String> {
    let mut handle =
        std::fs::File::open(inp_path).map_err(|reason| format!("Failed ot open {name} file because {reason:?}"))?;
    let (expected_cardinality, dimensionality) = {
        let car_dim = read_row::<_, u32>(&mut handle, 2)?;
        (car_dim[0] as usize, car_dim[1] as usize)
    };
    println!("Expecting to read {expected_cardinality} points in {dimensionality} dimensions from {name} set ...");

    let mut cardinality = 0;
    for (i, _) in (0..expected_cardinality).step_by(batch_size).enumerate() {
        // let batch_len = convert_batch_npy::<_, T>(&mut handle, batch_size, dimensionality, out_dir, name, i)?;
        let batch_len = convert_batch_arrow::<_, T>(&mut handle, batch_size, dimensionality, out_dir, name, i)?;
        cardinality += batch_len;
    }
    if expected_cardinality == cardinality {
        Ok([cardinality, dimensionality])
    } else {
        Err(format!(
            "Unable to read the correct number of points. Got {cardinality} but expected {expected_cardinality}."
        ))
    }
}

#[allow(dead_code)]
fn convert_batch_npy<R: Read, T: IntoArrowArray>(
    handle: &mut R,
    batch_size: usize,
    dimensionality: usize,
    out_dir: &Path,
    name: &str,
    i: usize,
) -> Result<usize, String> {
    println!("Converting batch {i} from {name} set to npy format ...");
    let mut batch = Array2::zeros((0, dimensionality));
    for _ in 0..batch_size {
        if let Ok(row) = read_row::<R, T>(handle, dimensionality) {
            let row = ArrayView::from(&row);
            batch
                .push_row(row)
                .map_err(|reason| format!("Could not push row to batch because {reason:?}."))?;
        } else {
            break;
        };
    }
    let out_path = {
        let mut path = out_dir.to_owned();
        path.push(format!("{name}-{i}.npy"));
        path
    };
    ndarray_npy::write_npy(out_path, &batch).map_err(|reason| format!("Could not write batch because {reason:?}."))?;
    Ok(batch.nrows())
}

fn convert_batch_arrow<R: Read, T: IntoArrowArray>(
    handle: &mut R,
    batch_size: usize,
    dimensionality: usize,
    out_dir: &Path,
    name: &str,
    i: usize,
) -> Result<usize, String> {
    println!("Converting batch {i} from {name} set to arrow format ...");
    let mut batch = Vec::new();
    for _ in 0..batch_size {
        if let Ok(row) = read_row::<R, T>(handle, dimensionality) {
            let row = T::into_arrow_array(&row);
            batch.push(row);
        } else {
            break;
        };
    }
    let num_rows = batch.len();

    let batch = RecordBatch::try_from_iter_with_nullable(
        batch
            .into_iter()
            .enumerate()
            .map(|(i, row)| (format!("{i}"), row, false))
            .collect::<Vec<_>>(),
    )
    .unwrap();

    let batch_file = {
        let mut path = out_dir.to_owned();
        path.push(format!("{name}-{i}.arrow"));
        std::fs::File::create(path).unwrap()
    };
    let mut writer = FileWriter::try_new(batch_file, batch.schema().as_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    Ok(num_rows)
}

fn read_row<R: Read, T: Number>(handle: &mut R, dimensionality: usize) -> Result<Vec<T>, String> {
    let num_bytes = T::num_bytes() * dimensionality;
    let mut row = vec![0_u8; num_bytes];

    handle
        .read_exact(&mut row)
        .map_err(|reason| format!("Could not read row from file because {:?}", reason))?;

    let row = row
        .chunks_exact(T::num_bytes())
        .map(|bytes| T::from_le_bytes(bytes))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(row)
}
