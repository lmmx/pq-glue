# 2026-10-24: The inputs' writer, version and sorting columns kept

Continues docs/journal/2026-10-23-row-group-copy.md.

## Current State

- The output keeps the inputs' `created_by`, format version and row groups' sorting
  columns when all the inputs have the same, in src/concat.rs: `created_by`,
  `writer_version` and `sorting_columns`, set in the writer's properties. Where the
  inputs differ, the output keeps parquet-rs's `created_by` and version, and its row
  groups declare no sorting columns.
- Tests: in Rust, arrow-rs inputs with a custom `created_by`, version 2 and sorting
  columns, and inputs whose sorting columns differ (tests/metadata.rs). In Python,
  pyarrow inputs written with `sorting_columns` (python/tests/test_metadata.py).

## Why

wikidata-pq's sort writes each key's files with pyarrow, single-threaded, in row groups
declaring `sorting_columns`. On the claims key of its benchmark (1,175,112 rows, 93
leaf columns, 8 row groups of 158,708 rows), writing each row group as a file of its
own, in 4 threads, took 2.97 s against 9.63 s for the one sequential write. All 744
column chunks were byte for byte the same: pyarrow's writer starts each row group's
column writers afresh, with their dictionaries, pages and content-defined chunking.

Gluing those files gave the same data, schema, key-value metadata and statistics as the
sequential write, but a different footer:

| | written whole (pyarrow) | glued, before | glued, now |
|---|---|---|---|
| `created_by` | parquet-cpp-arrow version 25.0.1 | parquet-rs version 60.0.0 | parquet-cpp-arrow version 25.0.1 |
| format version | 2.6 | 1.0 | 2.6 |
| sorting columns | column 0, ascending | none | column 0, ascending |

parquet-rs writes its own `created_by` and version, and takes every row group's sorting
columns from the writer's properties, which pq-glue left unset. Readers use `created_by`
to decide whether to trust a writer's statistics, so it should name the writer that
encoded the column chunks, not the one that copied them.

## Missing

- Row groups whose sorting columns differ can't keep theirs: parquet-rs's writer gives
  every row group the same. Patching the footer, as for column orders, would keep them.
