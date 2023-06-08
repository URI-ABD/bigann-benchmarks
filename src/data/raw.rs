use clam::number::Number;
use ndarray::prelude::*;
use std::io::Read;
use std::path::{Path, PathBuf};

use super::standard::StandardData;

#[derive(Debug, Clone)]
pub struct RawData<T: Number> {
    pub(crate) _t: std::marker::PhantomData<T>,
    pub(crate) base_path: PathBuf,
    pub(crate) query_path: PathBuf,
    pub(crate) ground_path: PathBuf,
}

impl<T: Number> RawData<T> {
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

fn convert_vectors<T: Number>(
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
        println!("Reading batch {i} from {name} set ...");
        let out_path = {
            let mut path = out_dir.to_owned();
            path.push(format!("{name}-{i}.npy"));
            path
        };
        let batch_len = convert_batch::<_, T>(&mut handle, batch_size, dimensionality, &out_path)?;
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

fn convert_batch<R: Read, T: Number>(
    handle: &mut R,
    batch_size: usize,
    dimensionality: usize,
    out_path: &Path,
) -> Result<usize, String> {
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
    ndarray_npy::write_npy(out_path, &batch).map_err(|reason| format!("Could not write batch because {reason:?}."))?;
    Ok(batch.nrows())
}

fn read_row<R: Read, T: Number>(handle: &mut R, dimensionality: usize) -> Result<Vec<T>, String> {
    let num_bytes = (T::num_bytes() as usize) * dimensionality;
    let mut row = vec![0_u8; num_bytes];

    handle
        .read_exact(&mut row)
        .map_err(|reason| format!("Could not read row from file because {:?}", reason))?;

    let row = row
        .chunks_exact(T::num_bytes() as usize)
        .map(|bytes| T::from_le_bytes(bytes))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(row)
}

// fn report_tree(root: &Cluster<f32, f32, VecVec<f32, f32>>, out_dir: &Path) {
//     let clusters = root.subtree();
//     let tree_array = {
//         let names: ArrayRef = {
//             let names = clusters.iter().map(|c| Some(c.name()));
//             Arc::new(StringArray::from_iter(names))
//         };

//         let depths = {
//             let depths = clusters.iter().map(|c| Some(c.depth() as u64));
//             Arc::new(UInt64Array::from_iter(depths))
//         };

//         let (lefts, rights) = {
//             let (lefts, rights): (Vec<_>, Vec<_>) = clusters
//                 .iter()
//                 .map(|c| match c.children() {
//                     Some([left, right]) => (Some(left.name()), Some(right.name())),
//                     None => (None, None),
//                 })
//                 .unzip();
//             (
//                 Arc::new(StringArray::from_iter(lefts)),
//                 Arc::new(StringArray::from_iter(rights)),
//             )
//         };

//         let cardinalities = {
//             let cardinalities = clusters.iter().map(|c| Some(c.cardinality() as u64));
//             Arc::new(UInt64Array::from_iter(cardinalities))
//         };

//         let arg_centers = {
//             let arg_centers = clusters.iter().map(|c| Some(c.arg_center() as u64));
//             Arc::new(UInt64Array::from_iter(arg_centers))
//         };

//         let arg_radii = {
//             let arg_radii = clusters.iter().map(|c| Some(c.arg_radius() as u64));
//             Arc::new(UInt64Array::from_iter(arg_radii))
//         };

//         let radii = {
//             let radii = clusters.iter().map(|c| Some(c.radius()));
//             Arc::new(Float32Array::from_iter(radii))
//         };

//         let lfds = {
//             let lfds = clusters.iter().map(|c| Some(c.lfd() as f32));
//             Arc::new(Float32Array::from_iter(lfds))
//         };

//         let polar_distances = {
//             let polar_distances = clusters.iter().map(|c| c.polar_distance());
//             Arc::new(Float32Array::from_iter(polar_distances))
//         };

//         RecordBatch::try_from_iter_with_nullable(vec![
//             ("name", names, false),
//             ("depth", depths, false),
//             ("left", lefts, true),
//             ("right", rights, true),
//             ("cardinality", cardinalities, false),
//             ("arg_center", arg_centers, false),
//             ("arg_radius", arg_radii, false),
//             ("radius", radii, false),
//             ("lfd", lfds, false),
//             ("polar_distance", polar_distances, true),
//         ])
//         .unwrap()
//     };

//     let tree_file = {
//         let mut path = out_dir.to_path_buf();
//         path.push("tree.arrow");
//         std::fs::File::create(path).unwrap()
//     };
//     let mut writer = FileWriter::try_new(tree_file, tree_array.schema().as_ref()).unwrap();
//     writer.write(&tree_array).unwrap();
//     writer.finish().unwrap();
//     println!("Wrote tree report ...");

//     let leaves = clusters.into_iter().filter(|c| c.is_leaf()).collect::<Vec<_>>();
//     let leaves_array = {
//         let names: ArrayRef = {
//             let names = leaves.iter().map(|c| Some(c.name()));
//             Arc::new(StringArray::from_iter(names))
//         };
//         let indices = {
//             let indices = leaves
//                 .iter()
//                 .map(|c| c.indices().into_iter().map(|i| Some(i as u64)).collect::<Vec<_>>())
//                 .map(Some);
//             Arc::new(LargeListArray::from_iter_primitive::<UInt64Type, _, _>(indices))
//         };

//         RecordBatch::try_from_iter_with_nullable(vec![("name", names, false), ("indices", indices, false)]).unwrap()
//     };
//     let leaves_file = {
//         let mut path = out_dir.to_path_buf();
//         path.push("leaves.arrow");
//         std::fs::File::create(path).unwrap()
//     };
//     let mut writer = FileWriter::try_new(leaves_file, leaves_array.schema().as_ref()).unwrap();
//     writer.write(&leaves_array).unwrap();
//     writer.finish().unwrap();
//     println!("Wrote leaves report ...");
// }
