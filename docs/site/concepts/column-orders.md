---
description: "Why pq-glue rewrites the footer's column orders, so that copied float statistics stay readable."
---

# Column orders

The footer of a Parquet file declares, for each leaf column, the sort order its min/max
statistics use. For most types this is the type-defined order. For floats, the Parquet
format also defines IEEE 754 total order, which orders NaN and signed zeros and requires
each column chunk's statistics to carry a NaN count.

## The problem with a plain copy

parquet-rs (v60) declares IEEE 754 total order for every float column of a file it
writes, whatever order the statistics in it were computed in. pyarrow writes float
statistics in the type-defined order, without NaN counts. After a plain copy of pyarrow
files, the output declares IEEE 754 total order for statistics that do not follow it,
and Arrow C++ (so pyarrow) ignores their min and max.

## What pq-glue does

After parquet-rs writes the output's footer, pq-glue rewrites its `column_orders` list
to the inputs' orders. In Thrift's compact encoding each entry is 3 bytes either way
(`1C 00 00` for the type-defined order, `2C 00 00` for IEEE 754), so the rewrite changes
no offsets or lengths.

The rewrite is checked:

- the list must be the footer's last field, with one entry per leaf column, each one of
  the two known encodings, or the output is rejected and deleted;
- after the rewrite the footer is read back, and its column orders must equal the inputs'.

Inputs written without column orders (by writers older than parquet-format 2.4) get the
type-defined order, which is how readers interpret such files.

The tests check every column chunk's statistics in the output against its source's, as
pyarrow reads them, with NaN, -0.0, +0.0 and infinities in the float columns.
