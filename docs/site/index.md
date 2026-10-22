---
description: "Concatenate Parquet files without decoding them."
---

# pq-glue

pq-glue copies the row groups of several Parquet files into one new file. Each column
chunk is copied as bytes: it is not decompressed, decoded or re-encoded. The time taken
depends on the number of bytes copied, not on the number of columns or how deeply they
are nested.

```python notest
import pq_glue

rows = pq_glue.concat(["a.parquet", "b.parquet"], "ab.parquet")
```

The inputs must share one Parquet schema. The output keeps the inputs' encodings,
compression, statistics, page indexes and bloom filters, and the first input's Arrow
schema (`ARROW:schema`).

On 66 files of nested Wikidata claims (555 MB), `concat` took 1.1 to 1.5 s, against 61 s
with Polars and 56 s with pyarrow ([benchmarks](https://github.com/lmmx/pq-glue/tree/master/bench)).

- [Installation](getting-started/installation.md)
- [Quickstart](getting-started/quickstart.md)
- [What is copied](concepts/copying.md)
- [API reference](reference/index.md)
