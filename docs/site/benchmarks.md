---
description: "Wall time, CPU time and peak memory of concatenating Parquet files with pq-glue, Polars and pyarrow, and how to reproduce them."
---

# Benchmarks

pq-glue against the two usual ways of concatenating Parquet files from Python:

| method | code |
|---|---|
| Polars | `pl.scan_parquet(src).sink_parquet(dst)` |
| pyarrow | each row group read with `ParquetFile.read_row_group` and written with `ParquetWriter.write_table` (zstd) |
| pq-glue | `pq_glue.concat(src, dst)` |

All figures are from a 4-core Intel Xeon at 2.1 GHz with Python 3.13. Ranges are the
lowest and highest of 3 rounds.

## Generated files

24 files from `bench/make_data.py`: 131 MB, 1.44 million rows, one row group per file.

| method | wall (s) | CPU (s) | peak RSS (MB) |
|---|---|---|---|
| Polars | 4.79–5.29 | 17.41–18.72 | 735–786 |
| pyarrow | 7.27–7.35 | 8.25–8.38 | 291–307 |
| pq-glue | 0.23–0.28 | 0.23–0.27 | 11–12 |

pq-glue took about 1/20 of Polars' wall time and 1/70 of its CPU time. Its peak memory
is that of the copy buffer and the footers: it does not grow with the number of rows.

## Wikidata claims

The claims partition files of 66 Wikidata chunks, from
[wikidata-pq](https://github.com/lmmx/wikidata-pq): 555 MB, 9,372,174 rows in 114 row
groups, 93 leaf columns nested up to lists of structs of lists. Measured in-process:

| method | wall (s) | CPU (s) | peak RSS |
|---|---|---|---|
| Polars | 61 | 238 | 2.2 GB |
| pyarrow | 56 | 74 | 0.8 GB |
| pq-glue | 1.1–1.5 | 0.6 | 87 MB (the whole Python process) |

Decoding and re-encoding cost more for deeply nested columns, where every value has
repetition and definition levels to rebuild. pq-glue does neither, so its time depends
only on the bytes copied. It was about 20 times faster than Polars on the flat generated
files, and about 50 times faster on the claims.

## Checking the outputs

In each benchmark, the first round's outputs must agree before any times are recorded:
the same row count, and the same two sums of position-seeded row hashes, read with Polars.
Any difference stops the run.

## How it is timed

- **Processes:** each method runs in a fresh Python process, in a different order each
  round.
- **Times:** wall and CPU time (user and system) cover the concatenation only, not the
  imports.
- **Memory:** peak RSS is the process's `VmHWM`, so the figures are for Linux only.

## Reproducing

The scripts are in
[bench/](https://github.com/lmmx/pq-glue/tree/master/bench), a uv project that installs
pq-glue from the repository:

```sh
cd bench
uv run make_data.py          # 24 files of 60,000 rows each in data/, about 131 MB
uv run run.py --rounds 3     # times each method and checks their outputs agree
```

To run the same comparison on your own files:

```sh
uv run run.py --files 'path/to/*.parquet'
```

Each run is appended to `bench/results.jsonl`.
