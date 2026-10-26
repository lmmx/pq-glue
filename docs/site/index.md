---
description: "Concatenate Parquet files without decoding them."
---

<div class="home-hero" markdown>

![The pq-glue logo: row-group planes arriving from two axes settle into one ordered stack inside a hairline lattice cube](assets/logo.svg){ .home-hero-logo width="180" }

<div class="home-hero-text" markdown>

# pq-glue

<p class="home-hero-tagline">Concatenate Parquet files without decoding them.</p>

</div>

</div>

pq-glue copies the row groups of several Parquet files into one new file. Each column
chunk is copied as bytes: it is not decompressed, decoded or re-encoded. The time it
takes depends on the number of bytes copied, not on the number of columns or how deeply
they are nested.

<!--
```python
import pyarrow as pa
import pyarrow.parquet as pq

claims = pa.schema(
    [
        ("id", pa.large_string()),
        ("property", pa.large_string()),
        ("qualifiers", pa.large_list(pa.struct([("property", pa.large_string()), ("amount", pa.float64())]))),
    ]
)
pq.write_table(
    pa.table(
        {"id": ["Q1", "Q2"], "property": ["P31", "P279"], "qualifiers": [[{"property": "P580", "amount": 1.5}], []]},
        schema=claims,
    ),
    "part-0.parquet",
)
pq.write_table(
    pa.table({"id": ["Q3"], "property": ["P31"], "qualifiers": [None]}, schema=claims),
    "part-1.parquet",
)
```
-->

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

- [`concat`][pq_glue.concat] returns the number of rows written.
- The output has each input's row groups, in input order: here one from each file.
- The inputs must have one Parquet schema. Files that differ are refused, and nothing
  is written.

## Where to go

- [Quickstart](getting-started/quickstart.md): write two files, concatenate them, and
  see what is refused.
- [What is copied](concepts/copying.md): every part of the output file, and where it
  comes from.
- [Column orders](concepts/column-orders.md): why the copied float statistics need a
  change to the footer, and how pq-glue makes it.
- [Column encodings](concepts/column-encodings.md): when to write a column in an
  encoding of its own, such as `DELTA_BYTE_ARRAY` for sorted ids, and what it saved on
  Wikidata's claims.
- [Benchmarks](benchmarks.md): pq-glue against Polars and pyarrow, on generated files
  and on Wikidata claims.

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
|---|---|
| column chunks | copied as bytes: the same pages, encodings and compression |
| statistics | each row group's min, max and null counts, readable by pyarrow |
| page indexes, bloom filters | copied where the inputs have them |
| schema | the first input's Parquet schema and `ARROW:schema` |
| column orders | the inputs', so float statistics stay valid |

## Performance

On the claims of 66 Wikidata chunks (555 MB, 9.4 million rows, 93 nested leaf columns),
`concat` took 1.1 to 1.5 s and 0.6 s of CPU. Polars took 61 s (238 s of CPU) and pyarrow
56 s. The [benchmarks](benchmarks.md) page has the full figures
and how to reproduce them.
