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

Hypotheses, not yet measured:

- That memory scales with the row groups in flight, so a `max_workers` bound trades it
  for speed in steps (to measure: 1, 2, 3, 4 workers).
- Why genson 1.3.3's wheel takes 1.8× the time and CPU of the same code, giving the
  same bytes, in pq-glue: its build (profile, features) or its process (mimalloc,
  which genson-core links; Polars' thread pool beside rayon's). Not the code. No
  allocator is changed here.

Under way:

- Rows and footers of the remaining outputs (without CDC; #45's).
- Deduplication: how much of a next release (every 97th entity dropped, a value
  changed in every 211th row) a store holding this one already has, by Parquet page and
  by Xet-style content-defined chunks of the file's bytes, for each writer with CDC.

## Missing

- **Streaming:** `write_parquet` collects the whole stream before encoding (the
  adaptive dictionary samples each leaf's first rows; row groups are cut from the
  batches). For a table or a frame that costs nothing (the buffers are taken over);
  for a lazy record batch reader it holds the stream. To do: encode row groups as the
  stream yields them, sampling the dictionary from the first row group.
- The engines: `engine=` and `max_workers=` on `write_parquet` (and `max_workers` in
  `WriteOptions`), the `pyarrow` engine as an optional extra.
- polars-genson calling `pq_glue::write::write_batches`, its writer removed, its tests
  passing and its output byte for byte the same. Publishing order: genson-core is on
  crates.io, so the pq-glue crate (with `write`) must be published first.
- wikidata-pq's sort on `pq_glue.write_parquet`, measured on the CI benchmark.
- User documentation (README, API docs, "which engine should I use"), from what is
  confirmed here once the engines exist.
