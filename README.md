# BigANN Benchmarks

This repo performs search benchmarks for [CAKES](https://github.com/URI-ABD/clam) on five of the BigANN datasets from the [NeurIPS 2021 competition](https://big-ann-benchmarks.com/).
We exclude the `Yandex Text-to-Image` dataset because the accompanying distance function (`inner-product`) is not a metric.

## Data Preparation

We first need to convert data from the raw binary format to the `arrow` format.

```shell
Usage: cargo run -r -- <DATASET> <RAW_DIR> <STANDARD_DIR>

Arguments:
  <DATASET>       Name of dataset. One of ["bigann", "fb_ssnnpp", "msft_spacev", "msft_turing", "yandex_deep"]
  <RAW_DIR>       Path to raw data directory.
  <STANDARD_DIR>  Path to standard data directory.
```

Examples:

1. `bigann`:
```shell
> cargo run -r -- bigann /data/raw/bigann /data/standard/bigann
```

2. `fb_ssnnpp`:
```shell
> cargo run -r -- fb_ssnnpp /data/raw/bigann /data/standard/bigann
```

3. `msft_spacev`:
```shell
> cargo run -r -- msft_spacev /data/raw/bigann /data/standard/bigann
```

4. `msft_turing`:
```shell
> cargo run -r -- msft_turing /data/raw/bigann /data/standard/bigann
```

5. `yandex_deep`:
```shell
> cargo run -r -- yandex_deep /data/raw/bigann /data/standard/bigann
```

## Benchmarks

TODO

## Citation

TODO
