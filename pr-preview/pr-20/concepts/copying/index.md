# What is copied

## The output file

| part | where it comes from |
| --- | --- |
| row groups | every input's, in input order, unchanged |
| column chunks | copied as bytes: the same pages, dictionary page, encodings ([Column encodings](<https://lmmx.github.io/pq-glue/pr-preview/pr-20/concepts/column-encodings/index.md>)) and compression |
| column chunk statistics | the inputs' (min, max, null count, distinct count) |
| page indexes | the inputs' column and offset indexes, where they have them |
| bloom filters | the inputs', where they have them |
| Parquet schema | the first input's |
| key-value metadata | the first input's, including `ARROW:schema` |
| column orders | the inputs' ([Column orders](<https://lmmx.github.io/pq-glue/pr-preview/pr-20/concepts/column-orders/index.md>)) |
| `created_by` | parquet-rs, which writes the footer |

Row groups are not merged or split: an output written from files of one small row group each has as many small row groups. Read it with a reader that handles many row groups, or rewrite it once if a few large row groups are needed.

## Requirements on the inputs

Every input must have the first input's:

- Parquet schema (field names, types, repetition and logical types);
- key-value metadata (so `ARROW:schema` describes every row group);
- column orders (so every row group's statistics follow the same order).

## Cases `concat` refuses

`concat` returns an error and writes nothing when:

| case | Python | Rust |
| --- | --- | --- |
| no inputs | `ValueError` | `Error::NoInputs` |
| the output path is an input path | `ValueError` | `Error::OutputIsInput` |
| an input differs in schema, metadata or column orders | `ValueError` | `Error::Incompatible` |
| an input is not a Parquet file | `ValueError` | `Error::Parquet` |
| an input cannot be opened | `OSError` | `Error::Io` |

If the copy fails after the output file has been created, the output file is deleted.
