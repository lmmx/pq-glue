# 2026-10-24: One writer, several engines

Continues docs/journal/2026-10-24-footer-fidelity.md.

## Current State

On branch `feat/write` (stacked on lmmx/pq-glue#1), not merged or released:

- `pq_glue::write::write_batches(path, schema, batches, &WriteOptions)` (feature `write`),
  in src/write.rs: polars-genson's Parquet writer, moved, not rewritten
  (`genson_core::parquet::{open_writer, compression, unrepeated_leaves, leaf_arrays,
  sample, write_batches}`, about 250 lines). `WriteOptions::default()` is genson's
  writer: zstd level 1, a dictionary for each leaf but the mostly distinct
  (adaptive dictionary), data pages ended at the page size limit alone, arrow-rs's row
  group size. New options: compression, `sorting_columns`, content-defined chunking
  (CDC), and `adaptive_dictionary: false`.
- Two schedules of the one encoder (arrow-rs's), chosen by whether CDC is on:
  - **columns:** genson's: each row group's leaf columns encoded in parallel
    (`ArrowColumnWriter`), a few row groups at a time, appended in order.
  - **row groups:** with CDC. arrow-rs keeps its chunker (`ContentDefinedChunker`,
    `write_with_chunker`) private to `ArrowWriter`, and carries it across row groups,
    so columns cannot be chunked in parallel through its public API. Each row group is
    encoded by an `ArrowWriter` of its own, into memory, a wave of up to one per thread
    at a time, and glued in order by concat's copy. Each row group's chunking starts
    afresh, as Arrow C++ (pyarrow) chunks.
- concat's copy is now `Glue` (src/concat.rs): inputs appended as they come, from files
  or from memory (`Input<R: ChunkReader>`), so the row-group schedule holds one wave's
  encoded bytes at a time, not the file's.
- Python: `pq_glue.write_parquet(data, path, *, row_group_size, compression,
  compression_level, spec_names, adaptive_dictionary, sorting_columns,
  content_defined_chunking)`, reading any Arrow PyCapsule stream (`__arrow_c_stream__`),
  the stream read and the batches encoded with the GIL released. No pyarrow: a Polars
  frame is written without pyarrow imported (tested). The capsule code is in
  src/python.rs alone: the Rust API takes `RecordBatch`es, and genson will call it
  directly, with no FFI.
- Default features unchanged: `concat` depends on `parquet` alone, without codecs.
  `write` adds arrow-array, arrow-schema, parquet's `arrow`, `zstd` and `snap`, rayon,
  rustc-hash, xxhash-rust and bytes (genson-core's versions); `python` adds `write`.
- Tests: genson's writer tests, moved (the column schedule's output byte for byte
  `ArrowWriter`'s, over 4 layouts × 7 row counts × 1, 3 and 8 threads; adaptive
  dictionary; page size limit), and new ones: the row-group schedule the same bytes at
  1, 3 and 8 threads, each row group's column chunks those of an `ArrowWriter` writing
  that row group alone with CDC, footer declarations, no rows, a failed write removed.
  Python: round trips (value for value, NaN and -0.0 by bit), Polars and record batch
  reader inputs, sorting columns, compression, CDC options, rejected inputs.

## Why

wikidata-pq's sort (#45) wrote each key with pyarrow, a row group per thread, glued by
pq-glue: 21% off the sort on the CI benchmark. pyarrow's writer has no parallelism of
its own; polars-genson's writer (arrow-rs) already encoded in parallel but lived in
genson, with no CDC. The aim (Louis): one native writer, owned by pq-glue, which genson
calls instead of keeping its own, and which wikidata-pq calls from Python, without
pyarrow for the encoding.

Alternatives considered:

- **A pyarrow thread pool in pq-glue** (`write_table`, pyarrow an optional extra):
  written, then dropped: it still encodes with pyarrow, and pq-glue would gain a writer
  that is pyarrow's with threads around it.
- **A new arrow-rs writer in pq-glue beside genson's:** two encoders to keep. Moving
  genson's keeps one.
- **Adding options to genson's writer and calling genson from wikidata-pq:** leaves the
  writer in a JSON-schema crate, for a use that has nothing to do with JSON.

Design decision (Louis): offer the strategies behind one function rather than pick a
winner, so a caller chooses by workload, hardware and compatibility:

| engine | what | for |
|---|---|---|
| `native` | the moved encoder, one thread | predictable CPU and memory |
| `parallel` | the same encoder, columns or row groups in parallel, `max_workers` bounding it | throughput, when cores and memory are free |
| `pyarrow` | pyarrow's writer (optional dependency, imported only for this engine) | pyarrow's exact encoding, its options |

`native` and `parallel` are one encoder and two schedules, not two writers: the
measurements below show their output byte for byte the same. Common to all engines:
decoded values, row order, the Arrow schema, row group sizes, compression codec and
level, sorting columns, CDC on or off. Not promised: identical bytes across engines
(page boundaries, dictionaries, `created_by`, format version differ, and are
documented). An option an engine lacks is an error, not ignored. No automatic choice
of engine yet: there is not the evidence for one.

## Measured

The wikidata-pq benchmark's claims key, as the sort writes it: 1,175,112 rows, 13
columns, 93 leaf columns, sorted by id, 8 row groups of 158,708 rows (the sort's
`_row_group_rows`). Read from Arrow IPC first; only the write is timed. A fresh process
per run; 3 runs each, the median. 4 cores, 15 GB (this session's VM, whose first touch
of fresh memory is slow: absolute times are this machine's). "RSS above the table":
the process's peak RSS (VmHWM) less its RSS with the table loaded. "Hub settings":
wikidata-pq's for the Hub's files (zstd level 3, page index, CDC, sorting by id).

| writer | threads | wall | CPU | RSS above the table | bytes |
|---|---|---|---|---|---|
| polars-genson 1.3.3 `write_parquet` | all | 3.50 s | 11.4 s | 869 MB | 74,229,143 |
| pq-glue, genson's defaults | all | 1.93 s | 6.1 s | 558 MB | 74,229,143 |
| pq-glue, genson's defaults | 1 | 5.63 s | 5.5 s | 223 MB | 74,229,143 |
| pq-glue, Hub settings | all | 2.84 s | 8.2 s | 551 MB | 75,504,737 |
| pq-glue, Hub settings | 1 | 7.06 s | 7.0 s | 189 MB | 75,504,737 |
| pq-glue, Hub settings without CDC | all | 2.01 s | 6.9 s | 567 MB | 74,953,132 |
| pyarrow, Hub settings (wikidata-pq master) | 1 | 9.87 s | 9.7 s | 4 MB | 77,869,762 |
| pyarrow, a row group per thread, glued (#45) | 4 | 3.26 s | 10.6 s | 55 MB | 77,854,737 |

Confirmed:

- **The move changed no byte:** pq-glue at genson's defaults writes the same file as
  polars-genson 1.3.3 (sha256 `dc1a46443650e613…`, whole file).
- **Threads change no byte:** pq-glue's output at 1 thread and at 4 is the same file,
  with and without CDC (`dc1a4644…`, `463d7838…`).
- **Rows:** Polars reads the same frame (`DataFrame.equals`, schema included) from
  genson's, pq-glue's and pq-glue's Hub-settings output as from pyarrow's.
- **Footer:** pq-glue's Hub-settings file declares the sorting column in every row
  group, has a page index, `created_by` parquet-rs version 60.0.0 and format version
  1.0 (pyarrow's: parquet-cpp-arrow 25.0.1, 2.6). Size 3% under pyarrow's (the adaptive
  dictionary).
- **Speed with CDC:** 2.84 s against #45's 3.26 s (1.15×) and pyarrow's sequential
  9.87 s (3.5×), on 23% less CPU than #45. One thread: 7.06 s, 1.4× pyarrow's.
- **Memory:** the parallel schedules hold up to a row group per thread in flight
  (levels and encoded pages): about 550 MB above the table here, at one thread about
  190–220 MB. pyarrow streams a row group at a time and holds next to nothing.

- **Rows, all writers:** Polars reads the same frame from every output above, #45's and
  pq-glue's without CDC included.

**Memory against workers** (`max_workers`, a pool of the write's own; 3 runs, median):

| workers | genson's defaults: wall / CPU / RSS above | Hub settings: wall / CPU / RSS above |
|---|---|---|
| 1 | 5.03 s / 4.9 s / 228 MB | 7.42 s / 7.2 s / 198 MB |
| 2 | 3.25 s / 5.7 s / 390 MB | 4.48 s / 7.8 s / 322 MB |
| 3 | 2.27 s / 5.7 s / 496 MB | 3.59 s / 7.8 s / 441 MB |
| 4 | 1.76 s / 5.9 s / 546 MB | 2.59 s / 7.9 s / 529 MB |

The same file at every worker count (`dc1a4644…`, `463d7838…`). Memory goes up by a
row group in flight a worker (110–160 MB for these 158,708-row groups); CPU stays
nearly flat, so the workers buy wall time, not extra work.

**Deduplication**, of a next release written by the same writer, by a store holding
this one: the bytes of the new file already in the old, by Parquet page (offset index)
and by Xet-style content-defined chunks of the file's bytes (gear hash, 8 KiB minimum,
64 KiB average, 128 KiB maximum; a stand-in for the Hub's store, not it). Two next
releases:

- **churn spread out:** every 97th entity dropped, a value changed in every 211th row.
- **churn in one place:** a contiguous 1% of rows dropped (at 40%), a value changed in
  a contiguous 0.5% (at 70%).

| writer | spread: pages / chunks reused | in one place: pages / chunks reused |
|---|---|---|
| pyarrow, CDC (wikidata-pq master) | 0.05% / 0.5% | 50.8% / 45.0% |
| pyarrow, a row group per thread, CDC (#45) | 0.05% / 0.5% | 50.8% / 45.0% |
| pq-glue, CDC | 0.03% / 0.5% | 50.9% / 47.8% |
| pq-glue, no CDC | 0.03% / 0.0% | 31.0% / 32.0% |

- pq-glue's chunking keeps what pyarrow's does, a little more by bytes.
- Churn spread through every page defeats every writer, CDC or not: no page is left as
  it was.
- In one place, about half is reused, not the 98% unchanged: row groups are cut by row
  count, so each row group after the edit starts at another row, and with it every
  page. CDC resynchronises within a row group, not across their boundaries. (A lever
  for later: row groups cut where the content says, not by count.)
- pyarrow's CDC files have 6,782 pages to pq-glue's 2,138: pyarrow also ends a page at
  20,000 rows, which genson's writer does not (data pages end at the size limit).

**The engines**: `write_parquet(engine="native" | "parallel" | "pyarrow")`, in
python/pq_glue/_write.py, tested: "native" and "parallel" write the same file at any
worker count (with and without CDC); "pyarrow" on 4 threads writes the column chunks,
row groups, sorting columns, `created_by` and format version of pyarrow on one. All
engines round-trip the test tables value for value, declare the sorting columns, and
reject what they lack (`adaptive_dictionary` for "pyarrow", `max_workers` for
"native").

**In wikidata-pq's sort** (its CI benchmark, quick tier, 3 forward/reverse rounds on
GitHub's 4-core runners; wikidata-pq #45 against #44, which sorts with pyarrow on one
thread): with each key written by `write_parquet(engine="parallel")` and the Hub
settings, the sort took 18.13 s → 13.74 s (−24.2%), wall with finalise −13.6%, parity
passed, peak RSS of all processes 3.50 → 3.64 GB (inside the noise), finalise CPU
+1.8%. The same PR with pyarrow on threads, glued, had measured −21.4%.

Hypotheses, not yet measured:

- Why genson 1.3.3's wheel takes 1.8× the time and CPU of the same code, giving the
  same bytes, in pq-glue. The release profiles are the same (codegen-units 1, thin LTO,
  opt-level 2). genson-core installs mimalloc as the global allocator; pq-glue uses the
  system allocator. To settle without changing either allocator: polars-genson built
  on pq-glue's writer runs the moved code in genson's own process.

## Missing

- **Streaming:** `write_parquet` collects the whole stream before encoding (the
  adaptive dictionary samples each leaf's first rows; row groups are cut from the
  batches). For a table or a frame that costs nothing (the buffers are taken over);
  for a lazy record batch reader it holds the stream. To do: encode row groups as the
  stream yields them, sampling the dictionary from the first row group.
- polars-genson calling `pq_glue::write::write_batches` (branch
  `feat/pq-glue-writer`: its writer removed, genson-core's Parquet tests passing),
  its output byte for byte the same and its speed, to measure. Publishing order: genson-core is on
  crates.io, so the pq-glue crate (with `write`) must be published first.
- User documentation (README, API docs, "which engine should I use"), from what is
  confirmed here once the engines exist.
