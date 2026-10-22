# pq-glue

[![PyPI](https://img.shields.io/pypi/v/pq-glue?color=%2300dc00)](https://pypi.org/project/pq-glue)
[![crates.io: pq-glue](https://img.shields.io/crates/v/pq-glue.svg)](https://crates.io/crates/pq-glue)
[![Documentation](https://img.shields.io/badge/docs-lmmx.github.io%2Fpq--glue-blue)](https://lmmx.github.io/pq-glue/)
[![MIT licensed](https://img.shields.io/crates/l/pq-glue.svg)](https://github.com/lmmx/pq-glue/blob/master/LICENSE)

Glue Parquet files together without decoding them.

`pq-glue` concatenates Parquet files that share a schema by copying their encoded row
groups into one file. Pages, dictionaries and statistics are copied byte for byte and never decoded or
re-encoded, so the cost is the bytes copied, however deeply the columns nest.

```sh
pip install pq-glue
```

```python
import pq_glue

rows = pq_glue.concat(["a.parquet", "b.parquet"], "ab.parquet")
```

```rust
let rows = pq_glue::concat(&[a, b], &out)?;
```

It is built on arrow-rs's `parquet` crate (`SerializedRowGroupWriter::append_column`, as
arrow-rs's `parquet-concat` CLI uses).

## What the output holds

- The inputs' row groups in input order, each column chunk as it was: the same pages,
  encodings, compression, dictionary and statistics (min/max, null count).
- The first input's Parquet schema and key-value metadata, including `ARROW:schema`.
  Arrow readers use that metadata to restore the files' Arrow types, such as
  `large_string` and nullability. arrow-rs's `parquet-concat` does not copy it.
- The inputs' column orders: the orders their statistics were computed in (see below).
- The inputs' page indexes (column and offset indexes) and bloom filters, where they have
  them.

## What it rejects

Nothing is written when any of these hold:

- The inputs differ in Parquet schema, key-value metadata or column orders.
- There are no inputs.
- The output is one of the inputs.

An output already written is removed if the copy fails part way. The function returns the
number of rows written.

Errors from Python:

- `ValueError` for incompatible inputs.
- `OSError` for a file that cannot be read.

## Column orders and float statistics

A Parquet file records, for each leaf column, the order its min/max statistics follow.

parquet-rs (v60) writes every float column's order as IEEE 754 total order, whatever order
the copied statistics follow. pyarrow, for example, computes float statistics in the
type-defined order, and Arrow C++ ignores IEEE 754 float min/max that have no NaN count. So
a plain row-group copy keeps the statistics, but pyarrow no longer sees them.

pq-glue puts the inputs' column orders back:

- **The patch:** after parquet-rs writes the footer, the `column_orders` list (its last
  field) is patched in place. Both encodings of a column order are 3 bytes, so no offset
  or length changes.
- **Checks:** every element is checked to be one of the two known encodings before it is
  rewritten, and the footer is read back to check it holds the inputs' orders.
- **Old inputs:** files written without column orders (before parquet-format 2.4) get the
  type-defined order, their legacy meaning.

## Benchmark

**Workload:** the claims partition files of 66 Wikidata chunks (wikidata-pq): 9,372,174 rows
in 114 row groups, 93 leaf columns nested up to lists of structs of lists, 555 MB of
zstd Parquet. One process, 4-core VM.

| | wall | CPU | peak RSS |
|---|---|---|---|
| Polars `scan_parquet(paths).sink_parquet(dst)` | 61 s | 238 s | 2.2 GB |
| pyarrow, row groups read and rewritten | 56 s | 74 s | 0.8 GB |
| `pq_glue.concat(paths, dst)` | 1.1–1.5 s | 0.6 s | 87 MB (the Python process) |

**Checks on that output:**
- Its rows' fingerprint (count and two sums of position-seeded row hashes, read by
  Polars) equals the inputs'.
- Its Arrow schema, metadata included, equals the inputs'.
- All 10,602 column chunks' statistics equal their sources', including 378 DOUBLE
  min/max.

In wikidata-pq's full pipeline (66 chunks, 6 workers, groups of 22 chunks), merging
the claims this way took the run from 180–185 s to 130–132 s.

## Tests

```sh
maturin develop --release
pytest
```

The tests write inputs with pyarrow and check the output against them:
- **Columns:** nested lists of structs, nulls, empty lists, dictionary-encoded and plain
  columns, and floats with NaN, -0.0 and +0.0.
- **Data:** equal tables in pyarrow and in Polars.
- **Footer:** the same Arrow schema and key-value metadata, and every row group's
  statistics (min/max, null count) equal to its source's.
- **Indexes:** column and offset indexes (filtered reads through them). Bloom filters are
  copied the same way but not yet tested.
- **Rejections:** each of the cases above.
