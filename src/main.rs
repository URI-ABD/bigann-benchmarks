use data::prepare::Dataset;
use std::path::PathBuf;

use clap::{arg, command, value_parser};

mod data;
mod utils;

fn main() -> Result<(), String> {
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
        Dataset::BigAnn => Dataset::convert_bigann(&raw_dir, &standard_dir)?,
        Dataset::FbSSNpp => Dataset::convert_fb_ssnpp(&raw_dir, &standard_dir)?,
        Dataset::MsftSpaceV => Dataset::convert_msft_spacev(&raw_dir, &standard_dir)?,
        Dataset::MsftTuring => Dataset::convert_msft_turing(&raw_dir, &standard_dir)?,
        Dataset::YandexDeep => Dataset::convert_yandex_deep(&raw_dir, &standard_dir)?,
    };

    Ok(())
}
