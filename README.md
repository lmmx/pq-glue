# pq-glue

[![PyPI](https://img.shields.io/pypi/v/pq-glue?color=%2300dc00)](https://pypi.org/project/pq-glue)
[![crates.io: pq-glue](https://img.shields.io/crates/v/pq-glue.svg)](https://crates.io/crates/pq-glue)
[![Documentation](https://img.shields.io/badge/docs-lmmx.github.io%2Fpq--glue-blue)](https://lmmx.github.io/pq-glue/)
[![MIT licensed](https://img.shields.io/crates/l/pq-glue.svg)](https://github.com/lmmx/pq-glue/blob/master/LICENSE)

Concatenate Parquet files without decoding them.

pq-glue copies the row groups of several Parquet files into one new file. Each column
chunk is copied as bytes: it is not decompressed, decoded or re-encoded. The time taken
depends on the number of bytes copied, not on the number of columns or how deeply they
are nested.

## Installation

Python (3.10 or later):

```sh
pip install pq-glue
```

Rust:

```sh
cargo add pq-glue
```

## Usage

Python:

```python
import pq_glue

rows = pq_glue.concat(["a.parquet", "b.parquet"], "ab.parquet")
```

Rust:

```rust
use std::path::{Path, PathBuf};

let inputs = [PathBuf::from("a.parquet"), PathBuf::from("b.parquet")];
let rows = pq_glue::concat(&inputs, Path::new("ab.parquet"))?;
```

`concat` returns the number of rows written.

## The output file

The output file contains:

- **Row groups:** every input's row groups, in input order. Each column chunk is
  unchanged: the same pages, dictionary page, encodings, compression and statistics.
- **Schema and metadata:** the first input's Parquet schema and key-value metadata. The
  key-value metadata includes `ARROW:schema`, which pyarrow and Polars use to restore
  Arrow types such as `large_string` and field nullability.
- **Column orders:** the inputs' column orders (see [Float statistics](#float-statistics)).
- **Indexes and filters:** the inputs' page indexes (column and offset indexes) and bloom
  filters, where the inputs have them.

## Requirements on the inputs

All inputs must have the same Parquet schema, key-value metadata and column orders.

`concat` writes nothing and returns an error when:

- the inputs differ in any of those;
- there are no inputs;
- the output path is one of the input paths;
- an input is not a Parquet file or cannot be opened.

In Python these raise `ValueError`, except a file that cannot be opened, which raises
`OSError`. In Rust they are variants of `pq_glue::Error`.

If the copy fails after the output file has been created, the output file is deleted.

## Float statistics

A Parquet file declares, for each column, the sort order used for that column's min/max
statistics (its column order).

parquet-rs, which pq-glue uses to write the file, declares IEEE 754 total order for every
float column. A plain copy would therefore pair statistics computed under one order with
a declaration of another. Files written by pyarrow, for example, use the type-defined
order. Arrow C++ (pyarrow) then ignores the min/max of those float columns, because IEEE
754 total order also requires NaN counts that the copied statistics do not have.

pq-glue keeps the inputs' column orders instead. After parquet-rs writes the footer,
pq-glue rewrites the footer's `column_orders` list in place:

- **Same length:** the two encodings are both 3 bytes, so no offsets or lengths change.
- **Checked:** every entry must be one of the two known encodings, or the output is
  rejected.
- **Verified:** the footer is read back and compared with the inputs' column orders.
- **Old files:** inputs written without column orders (before parquet-format 2.4) get the
  type-defined order, which is what such files meant.

## Performance

Concatenating 66 files (555 MB, 9.4 million rows, 93 nested leaf columns) took 1.1 to
1.5 s with pq-glue, against 61 s with Polars and 56 s with pyarrow. The method and the
scripts to reproduce it are in [bench/](bench/README.md).

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md). In short:

```sh
just py       # build the extension in development mode and run the Python tests
just test     # Rust tests (cargo nextest)
just pc       # formatting and lints
```

## License

MIT
