# pq-glue

Concatenate Parquet files without decoding them.

pq-glue copies the row groups of several Parquet files into one new file. Each column chunk is copied as bytes: it is not decompressed, decoded or re-encoded. The time it takes depends on the number of bytes copied, not on the number of columns or how deeply they are nested.

```pycon
>>> import pq_glue
>>> pq_glue.concat(["part-0.parquet", "part-1.parquet"], "claims.parquet")
3
>>> import pyarrow.parquet as pq
>>> pq.read_table("claims.parquet").column("id").to_pylist()
['Q1', 'Q2', 'Q3']
>>> pq.ParquetFile("claims.parquet").metadata.num_row_groups
2
```

- `concat` returns the number of rows written.
- The output has each input's row groups, in input order: here one from each file.
- The inputs must have one Parquet schema. Files that differ are refused, and nothing is written.

## Where to go

- [Quickstart](<https://lmmx.github.io/pq-glue/pr-preview/pr-4/getting-started/quickstart/index.md>): write two files, concatenate them, and see what is refused.
- [What is copied](<https://lmmx.github.io/pq-glue/pr-preview/pr-4/concepts/copying/index.md>): every part of the output file, and where it comes from.
- [Column orders](<https://lmmx.github.io/pq-glue/pr-preview/pr-4/concepts/column-orders/index.md>): why the copied float statistics need a change to the footer, and how pq-glue makes it.
- [Benchmarks](<https://lmmx.github.io/pq-glue/pr-preview/pr-4/benchmarks/index.md>): pq-glue against Polars and pyarrow, on generated files and on Wikidata claims.

## Install

```sh
pip install pq-glue
```

```sh
cargo add pq-glue
```

The Python package needs Python 3.10 or later and has no Python dependencies.

## Contents of the output file

| part of the file | in the output |
| --- | --- |
| column chunks | copied as bytes: the same pages, encodings and compression |
| statistics | each row group's min, max and null counts, readable by pyarrow |
| page indexes, bloom filters | copied where the inputs have them |
| schema | the first input's Parquet schema and `ARROW:schema` |
| column orders | the inputs', so float statistics stay valid |

## Performance

On the claims of 66 Wikidata chunks (555 MB, 9.4 million rows, 93 nested leaf columns), `concat` took 1.1 to 1.5 s and 0.6 s of CPU. Polars took 61 s (238 s of CPU) and pyarrow 56 s. The [benchmarks](<https://lmmx.github.io/pq-glue/pr-preview/pr-4/benchmarks/index.md>) page has the full figures and how to reproduce them.
