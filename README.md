# BigANN Benchmarks

This repo performs search benchmarks for [CAKES](https://github.com/URI-ABD/clam) on five of the BigANN datasets from the [NeurIPS 2021 competition](https://big-ann-benchmarks.com/).
We exclude the `Yandex Text-to-Image` dataset because the accompanying distance function (`inner-product`) is not a metric.

## Usage

```shell
Usage: cargo run -r -- <DATASET> <RAW_DIR> <STANDARD_DIR>

Arguments:
  <DATASET>       Name of dataset. One of ["bigann", "fb_ssnnpp", "msft_spacev", "msft_turing", "yandex_deep"]
  <RAW_DIR>       Path to raw data directory.
  <STANDARD_DIR>  Path to standard data directory.
```

For example (on Ark):
```shell
> cargo run -r -- bigann /data/raw/bigann /data/standard/bigann
```

## Benchmarks

TODO

## Citation

TODO
