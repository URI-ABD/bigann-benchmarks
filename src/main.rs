use std::{path::PathBuf, str::FromStr};

use clap::{arg, command, value_parser};

mod data;
mod prepare;
mod utils;

#[derive(Debug, Clone, Copy)]
enum Dataset {
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
    fn names() -> [String; 5] {
        [
            "bigann".to_string(),
            "fb_ssnnpp".to_string(),
            "msft_spacev".to_string(),
            "msft_turing".to_string(),
            "yandex_deep".to_string(),
        ]
    }
}

fn main() {
    let matches = command!()
        .arg(
            arg!(<DATASET>)
                .help(format!("Name of dataset. One of {:?}", Dataset::names()))
                .value_parser(value_parser!(Dataset)),
        )
        .arg(
            arg!(<RAW_DIR>)
                .help("Path to raw data directory.")
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            arg!(<STANDARD_DIR>)
                .help("Path to standard data directory.")
                .value_parser(value_parser!(PathBuf)),
        )
        .get_matches();

    let name = *matches.get_one::<Dataset>("DATASET").unwrap();
    let raw_dir = utils::validate_data_dir(matches.get_one::<PathBuf>("RAW_DIR").unwrap()).unwrap();
    let standard_dir = utils::validate_data_dir(matches.get_one::<PathBuf>("STANDARD_DIR").unwrap()).unwrap();

    match name {
        Dataset::BigAnn => prepare::bigann(&raw_dir, &standard_dir).unwrap(),
        _ => todo!(),
    };
}
