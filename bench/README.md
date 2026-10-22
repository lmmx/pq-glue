# Benchmarks

Concatenating Parquet files with Polars, pyarrow and pq-glue.

## Reproducing

From this directory (the project installs pq-glue from `..`):

```sh
uv run make_data.py          # 24 files of 60,000 rows each in data/, about 131 MB
uv run run.py --rounds 3     # times each method and checks their outputs agree
```

`run.py --files 'path/to/*.parquet'` runs the same comparison on other files.

## What is measured

**The methods, given the same files in sorted order:**

| method | code |
|---|---|
| polars | `pl.scan_parquet(src).sink_parquet(dst)` |
| pyarrow | each row group read with `ParquetFile.read_row_group` and written with `ParquetWriter.write_table` (zstd) |
| pq-glue | `pq_glue.concat(src, dst)` |

**How each run is timed:**
- Each method runs in a fresh Python process, in a different order each round.
- Wall time and CPU time (user and system) cover the concatenation only, not the
  imports.
- Peak RSS is the process's `VmHWM`, so the figures are Linux only.

**How the outputs are checked:** the first round's three outputs must have the same row
count and the same two sums of position-seeded row hashes, read with Polars. Any
difference stops the run.

**Recording:** every run is appended to `results.jsonl`.

## Results

**Generated files** (`make_data.py` defaults): 24 files, 131 MB, 1.44 million rows, each
file one row group. 4-core Intel Xeon at 2.1 GHz, Python 3.13, 3 rounds:

| method | wall (s) | CPU (s) | peak RSS (MB) |
|---|---|---|---|
| polars | 4.79–5.29 | 17.41–18.72 | 735–786 |
| pyarrow | 7.27–7.35 | 8.25–8.38 | 291–307 |
| pq-glue | 0.23–0.28 | 0.23–0.27 | 11–12 |

**Wikidata claims:** the claims partition files of 66 Wikidata chunks from
[wikidata-pq](https://github.com/lmmx/wikidata-pq): 555 MB, 9,372,174 rows in 114 row
groups, 93 leaf columns nested up to lists of structs of lists. Same machine, measured
in-process:

| method | wall (s) | CPU (s) | peak RSS |
|---|---|---|---|
| polars | 61 | 238 | 2.2 GB |
| pyarrow | 56 | 74 | 0.8 GB |
| pq-glue | 1.1–1.5 | 0.6 | 87 MB (whole Python process) |

**In wikidata-pq's pipeline** (66 chunks, 6 workers, groups of 22 chunks), merging each
group's claims with pq-glue instead of Polars cut the run from 180–185 s to 130–132 s.
