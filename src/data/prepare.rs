use std::path::{Path, PathBuf};
use std::str::FromStr;

use super::raw::RawData;

#[derive(Debug, Clone, Copy)]
pub enum Dataset {
    BigAnn,
    FbSSNpp,
    MsftSpaceV,
    MsftTuring,
    YandexDeep,
}

impl FromStr for Dataset {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "bigann" => Ok(Self::BigAnn),
            "fb_ssnnpp" => Ok(Self::FbSSNpp),
            "msft_spacev" => Ok(Self::MsftSpaceV),
            "msft_turing" => Ok(Self::MsftTuring),
            "yandex_deep" => Ok(Self::YandexDeep),
            _ => Err(format!(
                "Dataset cannot be created from name {}. Try one of {:?}",
                s,
                Self::names()
            )),
        }
    }
}

impl Dataset {
    pub fn names() -> [String; 5] {
        [
            "bigann".to_string(),
            "fb_ssnnpp".to_string(),
            "msft_spacev".to_string(),
            "msft_turing".to_string(),
            "yandex_deep".to_string(),
        ]
    }

    pub fn convert_bigann(raw_dir: &Path, standard_dir: &Path) -> Result<(), String> {
        let raw_data = RawData::from_dir(raw_dir, "bigann-1b.u8bin", "bigann-query.u8bin", "bigann-ground.bin");
        let out_dir = create_out_dir("bigann", standard_dir)?;
        raw_data.convert::<u8>(&out_dir, 10_000_000)?;
        Ok(())
    }

    pub fn convert_fb_ssnpp(raw_dir: &Path, standard_dir: &Path) -> Result<(), String> {
        let raw_data = RawData::from_dir(
            raw_dir,
            "fb_ssnpp-1b.u8bin",
            "fb_ssnpp-query.u8bin",
            "fb_ssnpp-ground.rangeres",
        );
        let out_dir = create_out_dir("fb_ssnpp", standard_dir)?;
        raw_data.convert::<u8>(&out_dir, 10_000_000)?;
        Ok(())
    }

    pub fn convert_msft_spacev(raw_dir: &Path, standard_dir: &Path) -> Result<(), String> {
        let raw_data = RawData::from_dir(
            raw_dir,
            "msft_spacev-1b.i8bin",
            "msft_spacev-query.i8bin",
            "msft_spacev-ground.bin",
        );
        let out_dir = create_out_dir("msft_spacev", standard_dir)?;
        raw_data.convert::<i8>(&out_dir, 10_000_000)?;
        Ok(())
    }

    pub fn convert_msft_turing(raw_dir: &Path, standard_dir: &Path) -> Result<(), String> {
        let raw_data = RawData::from_dir(
            raw_dir,
            "msft_turing-1b.fbin",
            "msft_turing-query.fbin",
            "msft_turing-ground.bin",
        );
        let out_dir = create_out_dir("msft_turing", standard_dir)?;
        raw_data.convert::<f32>(&out_dir, 1_000_000)?;
        Ok(())
    }

    pub fn convert_yandex_deep(raw_dir: &Path, standard_dir: &Path) -> Result<(), String> {
        let raw_data = RawData::from_dir(
            raw_dir,
            "yandex_deep-1b.fbin",
            "yandex_deep-query.fbin",
            "yandex_deep-ground.bin",
        );
        let out_dir = create_out_dir("yandex_deep", standard_dir)?;
        raw_data.convert::<f32>(&out_dir, 1_000_000)?;
        Ok(())
    }
}

fn create_out_dir(name: &str, standard_dir: &Path) -> Result<PathBuf, String> {
    let mut path = standard_dir.to_owned();
    path.push(name);
    if path.exists() {
        if !path.is_dir() {
            return Err(format!("Output path exists but is not a directory: {path:?}."));
        }
    } else {
        std::fs::create_dir(&path)
            .map_err(|reason| format!("Could not create output directory {path:?} because {reason:?}."))?;
    }
    Ok(path)
}
