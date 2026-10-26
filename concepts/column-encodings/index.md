# Column encodings

Since 0.3.0, `write_parquet` takes a `column_encoding` option. It writes named leaf columns in an encoding of your choice, with no dictionary. The values read back are the same: only the bytes in the file change. This page explains when that is worth doing, using the case it was added for: the `statement_id` column of Wikidata's claims. The option's full list of encodings and the types each takes are in the API reference.

## How strings are stored by default

The "native" and "parallel" engines give a column a dictionary unless its sampled values are more than three quarters distinct (`adaptive_dictionary`); the "pyarrow" engine gives every column one. A column of unique strings, such as one holding an identifier per row, is written `PLAIN` by the native engines: each value as a 4-byte length followed by its bytes. Compression (zstd by default) then works on those bytes, a page at a time.

## How DELTA\_BYTE\_ARRAY stores them

`DELTA_BYTE_ARRAY`, also known as incremental or front coding, stores each value as two parts:

- the length of the prefix it shares with the value before it;
- the rest of the value, its suffix.

The prefix lengths are stored as a run of small integers (`DELTA_BINARY_PACKED`). The suffixes are stored as `DELTA_LENGTH_BYTE_ARRAY`: their lengths packed the same way, then their bytes back to back. A reader rebuilds each value from the value before it.

Four statement ids, two entities' worth, in the order they are written:

| value | `PLAIN` stores | `DELTA_BYTE_ARRAY` stores (prefix length, suffix) |
| --- | --- | --- |
| `Q42$F078E5B3-F9A8-480E-B7AC-D97778CBBEF9` | 4 + 40 bytes | 0, `Q42$F078E5B3-F9A8-480E-B7AC-D97778CBBEF9` |
| `Q42$8E3C9A76-0B25-4D1E-9F3A-2C61D5A0E7B4` | 4 + 40 bytes | 4, `8E3C9A76-0B25-4D1E-9F3A-2C61D5A0E7B4` |
| `Q42$D1B5F2E0-7A4C-4B39-8E61-03F9C2A7D8E5` | 4 + 40 bytes | 4, `D1B5F2E0-7A4C-4B39-8E61-03F9C2A7D8E5` |
| `Q43$1C7E4A92-3F05-4E8B-A2D6-9B0F8C3E5D71` | 4 + 40 bytes | 2, `3$1C7E4A92-3F05-4E8B-A2D6-9B0F8C3E5D71` |

`Q42$` is stored once, not three times, and `Q43` shares `Q4` with the `Q42` before it. The saving is the shared prefixes, so it depends on the row order. The random part of each value is stored in full either way.

## Why Wikidata's `statement_id` gains

Every Wikidata statement has an id made of its entity's id, `$`, and a UUID. Three things make that column a good fit:

- **Every value is unique,** so a dictionary can't help, and the column is written `PLAIN`.
- **The files are sorted by entity id,** so an entity's statements are adjacent and share their `Q…$` prefix. Neighbouring entities' ids share most of their digits too.
- **The prefix is a large share of the value.** The entity id and `$` are up to 11 of its 40–47 bytes, and compressing `PLAIN` pages finds only part of that repetition.

## An example

Statement ids shaped like Wikidata's, written both ways:

```python
import random

import pyarrow as pa
import pyarrow.parquet as pq
import pq_glue

rng = random.Random(0)
ids, statements = [], []
for n in range(1, 20_001):
    for _ in range(rng.randint(1, 6)):
        u = f"{rng.getrandbits(128):032X}"
        ids.append(f"Q{n}")
        statements.append(f"Q{n}${u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}")
claims = pa.table({"id": ids, "statement_id": statements})

pq_glue.write_parquet(claims, "plain.parquet")
pq_glue.write_parquet(
    claims, "delta.parquet", column_encoding={"statement_id": "DELTA_BYTE_ARRAY"}
)


def column(path, name):
    """A column's encodings, whether it has a dictionary, and its bytes in the file."""
    meta = pq.ParquetFile(path).metadata
    i = [meta.schema.column(j).path for j in range(meta.num_columns)].index(name)
    chunks = [meta.row_group(r).column(i) for r in range(meta.num_row_groups)]
    size = sum(c.total_compressed_size for c in chunks)
    return chunks[0].encodings, chunks[0].has_dictionary_page, size
```

The `statement_id` column changes encoding, and the `id` column, which isn't named, keeps its dictionary:

```pycon
>>> column("plain.parquet", "statement_id")[:2]
(('PLAIN', 'RLE'), False)
>>> column("delta.parquet", "statement_id")[:2]
(('RLE', 'DELTA_BYTE_ARRAY'), False)
>>> column("delta.parquet", "id") == column("plain.parquet", "id")
True
```

(`RLE` here encodes the column's definition levels, which record its nulls.)

The column is smaller, and the file reads back with the same values and schema:

```pycon
>>> column("delta.parquet", "statement_id")[2] < column("plain.parquet", "statement_id")[2]
True
>>> pq.read_table("delta.parquet").equals(pq.read_table("plain.parquet"))
True
```

Here the column is about 7% smaller.

## When to consider it

`DELTA_BYTE_ARRAY` suits string or binary columns that are:

- mostly distinct, so they have no dictionary;
- written in an order where each value shares a prefix with the one before: sorted keys, paths, URLs, hierarchical ids, timestamps as text.

The other encodings `column_encoding` takes follow the same idea for other types: `DELTA_BINARY_PACKED` for sorted or slowly changing integers, `BYTE_STREAM_SPLIT` for floats.

It may not help, or may make the column larger, when:

- **neighbouring values share no prefix,** as with hashes or random ids. On Wikidata's claims, the reference and qualifier hashes (SHA-1 hex digests, often repeated) grew 7.7% and 10.7%;
- **a dictionary does better,** as with a column of few distinct values. Wikidata's `id` column, with a few repeated values per entity, grew 10%;
- **the rows are not in an order that groups shared prefixes.** The saving depends on the order the rows are written in;
- **pages are tiny.** Each page carries its own delta blocks' headers. In pq-glue's tests, with content-defined chunks of 256–1,024 bytes, `statement_id` was larger in `DELTA_BYTE_ARRAY` (539 kB against 528 kB). At the default chunk sizes it is smaller.

Measure it on your own data: write a file each way and compare the column's `total_compressed_size`, as the example does.

## Trade-offs

- **Smaller files, so fewer bytes to store, upload and download.** On a job bound by the network, those bytes can matter more than the writer's speed.
- **CPU can move either way.** On Wikidata's claims, writing showed no change beyond noise. Reading the column on its own took 13–17% more CPU than reading its `PLAIN` pages.
- **The physical bytes change, the logical data doesn't.** The values, Arrow schema, row groups and statistics read back the same in every engine. A file's checksum changes, though, so anything comparing files byte for byte will see a difference.
- **Readers need the encoding.** `DELTA_BYTE_ARRAY` has been in the Parquet format since 2.0. pyarrow, Polars and arrow-rs read it, and DuckDB supports it; an old or minimal reader may not.
- **`concat` keeps each input's encodings,** as it copies column chunks as bytes. Files written with and without the option glue together, and each row group keeps its own encoding.

## Measured on Wikidata

From [wikidata-pq's journal](<https://github.com/lmmx/wikidata-pq/blob/master/docs/journal/2026-10-26-statement-id-encoding.md>) (lmmx/wikidata-pq\#184), which took up the option for the claims' `statement_id`.

**Measured,** on a sample of 1,175,112 claims rows, written as the pipeline writes them:

|  | before | `statement_id` in `DELTA_BYTE_ARRAY` | change |
| --- | --- | --- | --- |
| group files: `statement_id` | 25.40 MB | 23.44 MB | −7.7% |
| group files: claims files | 73.14 MB | 71.18 MB | −2.7% |
| published files: `statement_id` | 27.14 MB | 24.03 MB | −11.4% |
| published files: claims files | 74.40 MB | 71.29 MB | −4.2% |

Every variant's values read back equal to the input's.

**Modelled,** by wikidata-pq's benchmark, scaling the sample's bytes up to a full release. These are projections, not measurements:

- **published files:** 128.9 → 124.6 GB a release;
- **the release's upload at 7 MB/s:** about 16 minutes shorter, the median of six paired rounds. A later run on the released packages modelled 13 minutes.
- **at 30 MB/s and above:** the saving is within the benchmark's noise.

Choosing the right representation for one column saved more time than making the writer faster would have.
