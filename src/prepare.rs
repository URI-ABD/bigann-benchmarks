use std::path::Path;

use super::data::raw::RawData;

pub fn bigann(raw_dir: &Path, standard_dir: &Path) -> Result<(), String> {
    let raw_data = RawData::<u8>::from_dir(
        raw_dir,
        "bigann-1B.u8bin",
        "bigann-query.u8bin",
        "bigann-ground.bin",
        128,
    );

    raw_data.convert(standard_dir, 100_000)?;

    Ok(())
}
