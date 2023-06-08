use std::path::Path;

use super::data::raw::RawData;

pub fn bigann(raw_dir: &Path, standard_dir: &Path) -> Result<(), String> {
    let raw_data = RawData::<u8>::from_dir(raw_dir, "bigann-1B.u8bin", "bigann-query.u8bin", "bigann-ground.bin");

    let out_dir = {
        let mut path = standard_dir.to_owned();
        path.push("bigann");
        if path.exists() {
            if !path.is_dir() {
                return Err(format!("Output path exists but is not a directory: {path:?}."));
            }
        } else {
            std::fs::create_dir(&path)
                .map_err(|reason| format!("Could not create output directory {path:?} because {reason:?}."))?;
        }
        path
    };
    raw_data.convert(&out_dir, 10_000_000)?;

    Ok(())
}
