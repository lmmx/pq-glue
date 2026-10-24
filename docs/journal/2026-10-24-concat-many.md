# 2026-10-24: Many concatenations in one call

Continues docs/journal/2026-10-24-writer-engines.md.

## Current State

- `pq_glue::concat_many(jobs: &[(Vec<PathBuf>, PathBuf)]) -> Vec<Result<u64>>` (feature
  `parallel`, in src/concat.rs) runs a `concat` per job on the calling rayon pool and
  returns each job's result in the jobs' order. Every job runs whether or not another
  fails; a failed job leaves no output of its own, as `concat` leaves none.
- The `parallel` feature adds rayon alone; `python` enables it. Default features add
  `bytes` (already a dependency of parquet) and nothing else: no codecs.
- Python: `pq_glue.concat_many(jobs)` takes `(sources, dst)` tuples, releases the GIL for
  the whole batch and returns the rows of each job (src/python.rs, python/pq_glue/_pq_glue.pyi).
  The first failed job in the jobs' order raises `concat`'s exception type (OSError for
  I/O, ValueError otherwise), its message starting with the job's `dst` and "not written".
- `concat` reads its inputs whole into memory, one `read` each, when none is larger than
  16 MiB (`READ_WHOLE_BYTES`, src/concat.rs); larger inputs are read from the file as
  before. parquet-rs's `ChunkReader` for `File` duplicates the file handle, seeks and reads
  for every footer, page index, bloom filter and column chunk, and closes the duplicate.
- `footer::restore_column_orders` checks the patched footer by decoding the bytes it wrote
  (`ParquetMetaDataReader::decode_metadata`), not by opening the file again and reading its
  footer back (src/footer.rs).
- Tests: in Rust, each of 24 jobs' output byte for byte `concat`'s, no jobs, two failed jobs
  of five (another schema, a missing input) with the other three written (tests/many.rs);
  the output byte for byte the same with inputs read whole and read from the file
  (src/concat.rs). In Python, the same per job, and the exceptions and their messages
  (python/tests/test_many.py). CI runs `cargo test --features parallel` besides `cargo test`
  (.github/workflows/test.yml).

## Why

wikidata-pq's group close (`push.merge_group`) glues each table's languages into one file
per language: on its CI benchmark's quick tier (4 chunks, 1 group) 2,324 concatenations of
a few small files each (labels 564, descriptions 442, aliases 489, links 827). A `concat`
per language from a Python thread pool spent the merge on Python and on the GIL between
calls, and on system calls within them:

| system calls per job (strace -c of 500 jobs of 4 inputs, the whole process / 500) | `concat`, pq-glue e47f28b | `concat_many`, inputs read whole, footer decoded |
|---|---|---|
| all | 123 | 33 |
| `close` | 29 | 6 |
| `fcntl` (handle duplicated) | 22 | 0 |
| `lseek` | 26 | 4 |
| `read` | 24 | 6 |
| `openat` | 7 | 6 |

The merge of the benchmark's one group, on a snapshot of its partition files taken just
before the close (hard-linked afresh for each run, 10 runs each, interleaved, 4 CPUs):

| merge_group | median | range |
|---|---|---|
| a `concat` per language, in threads (wikidata-pq master, pq-glue e47f28b) | 1.94 s | 1.57–2.59 s |
| one `concat_many` for every glued table | 1.21 s | 0.95–1.48 s |
| the same, inputs read whole and footer decoded | 1.13 s | 1.00–1.33 s |

In wikidata-pq's whole pipeline benchmark (bench/pipeline.py, the quick tier's 4 chunks,
before and after run back to back, 4 pairs), the close's merge_wall went 1.66 → 1.11,
1.54 → 1.23, 1.96 → 1.79 and 1.87 → 1.67 s (medians 1.77 → 1.45 s), the VM's load
rising through the runs.

Of the rest, making the 2,324 languages' staging directories took most: about 0.3 ms of
system time per `mkdir` on this VM, made one at a time within a directory.
