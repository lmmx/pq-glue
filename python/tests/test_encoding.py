"""write_parquet's column_encoding.

A leaf column in an encoding of its own changes the file's bytes only, and concat copies
it as it is.
"""

import random
from typing import Any

import polars as pl
import pq_glue
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ENGINES = [("native", None), ("parallel", None), ("parallel", 3), ("pyarrow", None)]


def claims_like(rows: int = 30_000, seed: int = 0) -> pa.Table:
    """Rows shaped as Wikidata's claims.

    Sorted ids, a few statements an id, each statement id its row's id, `$` and a UUID,
    and a list of references' hashes (40 hex digits, often repeated).
    """
    rng = random.Random(seed)
    ids, statements, refs = [], [], []
    n = 1
    while len(ids) < rows:
        n += rng.randint(1, 50)
        for _ in range(rng.randint(1, 12)):
            u = f"{rng.getrandbits(128):032X}"
            ids.append(f"Q{n}")
            statements.append(f"Q{n}${u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}")
            refs.append(
                [f"{rng.getrandbits(160) % 50:040x}" for _ in range(rng.randint(0, 3))]
            )
    ids, statements, refs = ids[:rows], statements[:rows], refs[:rows]
    statements[5] = None
    return pa.table(
        {
            "id": pa.array(ids, pa.large_string()),
            "statement_id": pa.array(statements, pa.large_string()),
            "rank": pa.array(["normal"] * rows, pa.large_string()),
            "refs": pa.array(refs, pa.large_list(pa.large_string())),
        }
    )


DELTA = {"statement_id": "DELTA_BYTE_ARRAY"}


def chunk_meta(path, column: str) -> list:
    """The column's chunks' metadata, row group by row group."""
    md = pq.read_metadata(path)
    names = [md.schema.column(i).path for i in range(md.num_columns)]
    return [
        md.row_group(g).column(names.index(column)) for g in range(md.num_row_groups)
    ]


@pytest.mark.parametrize("engine,workers", ENGINES)
@pytest.mark.parametrize("cdc", [None, True], ids=["columns", "chunked"])
def test_only_the_bytes_change(tmp_path, engine, workers, cdc):
    """The column is written in its encoding with no dictionary, and only the bytes change.

    pyarrow and Polars read back the same values and schema as without it, the row
    groups and every column's statistics are the same, and the file is smaller.
    """
    t = claims_like()
    common: dict[str, Any] = dict(
        engine=engine,
        max_workers=workers,
        row_group_size=7_000,
        compression_level=3,
        content_defined_chunking=cdc,
        sorting_columns=["id"],
    )
    if engine == "pyarrow":
        common["adaptive_dictionary"] = False
    plain, delta = tmp_path / "plain.parquet", tmp_path / "delta.parquet"
    assert pq_glue.write_parquet(t, plain, **common) == t.num_rows
    assert (
        pq_glue.write_parquet(t, delta, column_encoding=DELTA, **common) == t.num_rows
    )

    for chunk in chunk_meta(delta, "statement_id"):
        assert "DELTA_BYTE_ARRAY" in chunk.encodings, chunk.encodings
        assert "PLAIN" not in chunk.encodings and not chunk.has_dictionary_page
    for chunk in chunk_meta(plain, "statement_id"):
        assert "DELTA_BYTE_ARRAY" not in chunk.encodings
    assert [c.encodings for c in chunk_meta(delta, "rank")] == [
        c.encodings for c in chunk_meta(plain, "rank")
    ]

    a, b = pq.read_table(plain), pq.read_table(delta)
    assert b.schema.equals(a.schema, check_metadata=True)
    assert b.equals(a) and b.equals(t.cast(b.schema))
    assert pl.read_parquet(delta).equals(pl.read_parquet(plain))
    assert (
        pl.scan_parquet(delta)
        .filter(pl.col("statement_id").str.starts_with("Q4"))
        .collect()
        .equals(
            pl.scan_parquet(plain)
            .filter(pl.col("statement_id").str.starts_with("Q4"))
            .collect()
        )
    )

    ma, mb = pq.read_metadata(plain), pq.read_metadata(delta)
    assert mb.schema.equals(ma.schema) and mb.num_row_groups == ma.num_row_groups
    for g in range(ma.num_row_groups):
        ra, rb = ma.row_group(g), mb.row_group(g)
        assert rb.num_rows == ra.num_rows
        assert rb.sorting_columns == ra.sorting_columns
        for i in range(ma.num_columns):
            sa, sb = ra.column(i).statistics, rb.column(i).statistics
            assert (sa is None) == (sb is None)
            if sa is not None:
                assert (sb.has_min_max, sb.null_count) == (
                    sa.has_min_max,
                    sa.null_count,
                )
                if sa.has_min_max:
                    assert (sb.min, sb.max) == (sa.min, sa.max)

    size = lambda p: sum(c.total_compressed_size for c in chunk_meta(p, "statement_id"))
    assert size(delta) < 0.95 * size(plain), (size(delta), size(plain))
    assert delta.stat().st_size < plain.stat().st_size


def test_nested_leaf(tmp_path):
    """A leaf in a list takes its path in the Parquet schema."""
    t = claims_like(5_000)
    out = tmp_path / "out.parquet"
    pq_glue.write_parquet(
        t, out, column_encoding={"refs.list.element": "delta_byte_array"}
    )
    for chunk in chunk_meta(out, "refs.list.element"):
        assert "DELTA_BYTE_ARRAY" in chunk.encodings and not chunk.has_dictionary_page
    assert pq.read_table(out).equals(t)


@pytest.mark.parametrize("engine", ["parallel", "pyarrow"])
def test_concat_copies_the_encoding(tmp_path, engine):
    """Concat copies the column chunks as they are.

    Files in DELTA_BYTE_ARRAY glue to a file in it, and glue with files that are not to
    one of both, its values the inputs'.
    """
    t = claims_like(12_000)
    opts: dict[str, Any] = dict(
        engine=engine, row_group_size=4_000, content_defined_chunking=True
    )
    if engine == "pyarrow":
        opts["adaptive_dictionary"] = False
    parts = []
    for i, enc in enumerate([DELTA, DELTA, None]):
        p = tmp_path / f"{i}.parquet"
        pq_glue.write_parquet(t.slice(4_000 * i, 4_000), p, column_encoding=enc, **opts)
        parts.append(p)
    out = tmp_path / "out.parquet"
    assert pq_glue.concat(parts[:2], out) == 8_000
    assert all(
        "DELTA_BYTE_ARRAY" in c.encodings for c in chunk_meta(out, "statement_id")
    )
    glued = pq.read_table(out)
    assert glued.equals(t.slice(0, 8_000).cast(glued.schema))
    assert pq_glue.concat(parts, out) == 12_000
    encodings = [c.encodings for c in chunk_meta(out, "statement_id")]
    assert ["DELTA_BYTE_ARRAY" in e for e in encodings] == [True, True, False]
    glued = pq.read_table(out)
    assert glued.equals(t.cast(glued.schema))


@pytest.mark.parametrize("engine", ["native", "pyarrow"])
@pytest.mark.parametrize(
    "encoding,says",
    [
        ({"nope": "DELTA_BYTE_ARRAY"}, "no leaf column"),
        ({"statement_id": "DELTA_BINARY_PACKED"}, "does not encode BYTE_ARRAY"),
        ({"statement_id": "NOT_AN_ENCODING"}, "no encoding"),
        ({"statement_id": "RLE_DICTIONARY"}, "not an encoding to ask for"),
    ],
)
def test_bad_encodings_raise(tmp_path, engine, encoding, says):
    """Bad encodings raise ValueError, in every engine, and leave no file.

    An unknown column, an unknown encoding, one the column's type does not take and a
    dictionary encoding are refused.
    """
    out = tmp_path / "out.parquet"
    opts: dict[str, Any] = {"adaptive_dictionary": False} if engine == "pyarrow" else {}
    with pytest.raises(ValueError, match=says):
        pq_glue.write_parquet(
            claims_like(500), out, engine=engine, column_encoding=encoding, **opts
        )
    assert not out.exists()
