---
description: "Write two Parquet files with pyarrow, concatenate them with pq_glue.concat, and check the result."
---

# Quickstart

Write two files with the same schema:

```python
import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema([("id", pa.string()), ("tags", pa.list_(pa.string()))])
pq.write_table(pa.table({"id": ["Q1", "Q2"], "tags": [["a"], []]}, schema=schema), "a.parquet")
pq.write_table(pa.table({"id": ["Q3"], "tags": [None]}, schema=schema), "b.parquet")
```

Concatenate them. `concat` returns the number of rows written:

```pycon
>>> import pq_glue
>>> pq_glue.concat(["a.parquet", "b.parquet"], "ab.parquet")
3
```

The output has both files' rows, in order, and one row group per input row group:

```pycon
>>> pq.read_table("ab.parquet").to_pylist()
[{'id': 'Q1', 'tags': ['a']}, {'id': 'Q2', 'tags': []}, {'id': 'Q3', 'tags': None}]
>>> pq.ParquetFile("ab.parquet").metadata.num_row_groups
2
```

Many concatenations can be done in one call: each job is a `concat`, with the same
output, and the jobs run in parallel. `concat_many` returns each job's rows:

```pycon
>>> pq_glue.concat_many([(["a.parquet", "b.parquet"], "ab2.parquet"), (["b.parquet"], "b2.parquet")])
[3, 1]
>>> open("ab2.parquet", "rb").read() == open("ab.parquet", "rb").read()
True
```

Files with different schemas are refused, and no output is written:

```pycon
>>> other = pa.table({"id": ["Q4"]})
>>> pq.write_table(other, "c.parquet")
>>> pq_glue.concat(["a.parquet", "c.parquet"], "ac.parquet")
Traceback (most recent call last):
  ...
ValueError: c.parquet has other schema than a.parquet
>>> import os
>>> os.path.exists("ac.parquet")
False
```
