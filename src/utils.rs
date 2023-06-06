use std::path::{Path, PathBuf};

pub fn validate_data_dir(dir: &Path) -> Result<PathBuf, String> {
    let dir = dir
        .canonicalize()
        .map_err(|reason| format!("Could not canonicalize path because {:}", reason))?;
    if !dir.exists() {
        Err(format!("Path does not exist: {:?}", dir))
    } else if !dir.is_dir() {
        Err(format!("Path is not a directory: {:?}", dir))
    } else {
        Ok(dir)
    }
}
