use std::io::Read;
use std::path::{Path, PathBuf};

use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use ndarray::prelude::*;

use super::number_to_arrow::IntoArrowArray;
use super::standard::StandardData;

#[derive(Debug, Clone)]
pub(crate) struct RawData {
    pub base_path: PathBuf,
    pub query_path: PathBuf,
    pub ground_path: PathBuf,
}

impl RawData {
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
        };

        assert!(data.base_path.exists(), "Path not found: {:?}", data.base_path);
        assert!(data.query_path.exists(), "Path not found: {:?}", data.query_path);
        assert!(data.ground_path.exists(), "Path not found: {:?}", data.ground_path);

        data
    }

    pub fn convert<T: IntoArrowArray>(
        &self,
        out_dir: &Path,
        batch_size: usize,
        knn: bool,
    ) -> Result<StandardData<T>, String> {
        // let [cardinality, dimensionality] = convert_vectors::<T>(&self.base_path, batch_size, "base", out_dir)?;
        let [num_queries, _] = convert_vectors::<T>(&self.query_path, batch_size, "query", out_dir)?;
        if knn {
            let [ground_queries, _] = convert_ground_knn(&self.ground_path, "ground", out_dir)?;
            if num_queries != ground_queries {
                return Err(format!(
                    "Number of queries ({}) does not match number of ground truth queries ({}).",
                    num_queries, ground_queries
                ));
            }
        } else {
            todo!()
        }

        let data = StandardData {
            _t: Default::default(),
            data_dir: out_dir.to_owned(),
            dimensionality: 0,
            cardinality: 0,
            batch_size,
            num_queries,
        };

        Ok(data)
    }
}

fn convert_ground_knn(inp_path: &Path, name: &str, out_dir: &Path) -> Result<[usize; 2], String> {
    let mut handle =
        std::fs::File::open(inp_path).map_err(|reason| format!("Failed ot open {name} file because {reason:?}"))?;
    let (num_queries, k) = {
        let q_k: Vec<u32> = read_row::<_, u32>(&mut handle, 2)?;
        (q_k[0] as usize, q_k[1] as usize)
    };
    println!("Expecting to read ground truth from {name} set with {num_queries} queries and k = {k} ...");
    println!("Converting ground truth data from {name} set to arrow format ...");
    let neighbors = read_row::<_, u32>(&mut handle, num_queries * k)?;
    let distances = read_row::<_, f32>(&mut handle, num_queries * k)?;
    if neighbors.len() != distances.len() {
        return Err(format!(
            "Number of neighbors ({}) does not match number of distances ({}).",
            neighbors.len(),
            distances.len()
        ));
    }

    let neighbors = RecordBatch::try_from_iter_with_nullable(
        neighbors
            .chunks_exact(k)
            .map(u32::into_arrow_array)
            .enumerate()
            .map(|(i, row)| (format!("{i}"), row, false))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let writer = {
        let mut path = out_dir.to_owned();
        path.push("neighbors.arrow");
        std::fs::File::create(path).unwrap()
    };
    let mut writer = FileWriter::try_new(writer, neighbors.schema().as_ref()).unwrap();
    writer.write(&neighbors).unwrap();
    writer.finish().unwrap();

    let distances = RecordBatch::try_from_iter_with_nullable(
        distances
            .chunks_exact(k)
            .map(f32::into_arrow_array)
            .enumerate()
            .map(|(i, row)| (format!("{i}"), row, false))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let writer = {
        let mut path = out_dir.to_owned();
        path.push("distances.arrow");
        std::fs::File::create(path).unwrap()
    };
    let mut writer = FileWriter::try_new(writer, distances.schema().as_ref()).unwrap();
    writer.write(&distances).unwrap();
    writer.finish().unwrap();

    Ok([num_queries, k])
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

fn read_row<R: Read, T: IntoArrowArray>(handle: &mut R, dimensionality: usize) -> Result<Vec<T>, String> {
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
