# 2026-10-23: Row-group copy, and the float statistics it hid

## Current State

### Why pq-glue exists

wikidata-pq merges each group's partition files into one file per key before uploading
them (`push.merge_group`). Its claims table is one key with 93 leaf columns, nested up to
lists of structs of lists. Polars' `scan_parquet(paths).sink_parquet(dst)` decodes and
re-encodes every column. For 66 chunks' claims files (555 MB, 9,372,174 rows) that took:

| method | wall | CPU | peak RSS |
|---|---|---|---|
| Polars sink | 61 s | 238 s | 2.2 GB |
| pyarrow, row groups re-written | 56 s | 74 s | 0.6–0.8 GB |
| encoded row groups copied (`append_column`) | 0.6 s | 0.5 s | 21 MB |

In wikidata-pq's whole pipeline (66 chunks, 6 workers, groups of 22 chunks) the merges
used 270 s of CPU against 385 s for processing every chunk, and the last group's merge
took about 60 s after the last chunk finished. With the copy, the run went from 180–185 s
to 130–132 s.

### How it copies

`concat` (src/concat.rs) does the following:

1. Opens each input and reads its footer and page indexes (src/input.rs).
2. Checks that every input has the first one's schema, column orders and key-value
   metadata.
3. Appends each column chunk with `SerializedRowGroupWriter::append_column`, its bloom
   filter and page index entries with it.
4. Restores the inputs' column orders in the footer (src/footer.rs).

This is the method of arrow-rs's `parquet-concat` CLI, which does not copy key-value
metadata. Without `ARROW:schema`, pyarrow reads `large_string` columns back as `string`,
and wikidata-pq's compaction then finds group files with two schemas.

### The float statistics

The first copies lost the min/max of every DOUBLE column as pyarrow read them: 378 of
the 10,602 column chunks of the 66 claims files. parquet-rs read the same statistics
back intact.

The cause is the footer's column orders. parquet-rs 60 writes IEEE 754 total order for
every float column (`ColumnOrder::column_order_for_type`, in its metadata writer). The
copied statistics come from pyarrow files, which declare the type-defined order and write
no NaN counts. Arrow C++ ignores IEEE 754 float min/max without NaN counts.

**What did not fix it:**
- Marking the statistics exact (`is_min_value_exact`).
- Copying the inputs' `created_by`.

**What fixed it:** neither of parquet-rs's metadata writers lets the column orders be
set, so `restore_column_orders` patches the written footer.
- **Why it is safe:** both encodings are 3 bytes in Thrift's compact protocol, and the
  `column_orders` list is the footer's last field.
- **Checks:** each entry must be one of the two encodings, and the footer is read back
  and compared with the inputs' orders.
- **Result:** all 10,602 column chunks' statistics then equal their sources' in pyarrow.
- **Test sensitivity:** with the patch disabled, the two statistics tests fail.

### Tests

| suite | what it checks |
|---|---|
| Rust `tests/data.rs` | every output column chunk's bytes equal its source's; decoded rows equal |
| Rust `tests/metadata.rs` | schema, key-value metadata, column orders, statistics, encodings, sizes, page indexes, bloom filters |
| Rust `tests/errors.rs` | each rejected case; the output is removed |
| Python `python/tests/` | the same against pyarrow-written inputs, with NaN, -0.0, +0.0 and infinities, bit-exact through Arrow IPC |

## Missing

- **Row groups:** they are not merged. Inputs of one small row group each give an output
  of many small row groups.
- **Encryption:** encrypted files are not supported (parquet-rs's `encryption` feature
  is off).
