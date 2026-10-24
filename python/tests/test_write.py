"""write_parquet: Arrow data written with arrow-rs, encoded in parallel, without pyarrow."""

import struct
import subprocess
import sys

import polars as pl
import pq_glue
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typing import Literal

from .conftest import table


def canon(v):
    """`v` with each float as its bits, so NaN equals NaN and -0.0 differs from 0.0."""
    if isinstance(v, float):
        return ("float", struct.pack("<d", v))
    if isinstance(v, list):
        return [canon(x) for x in v]
    if isinstance(v, dict):
        return {k: canon(x) for k, x in v.items()}
    return v


def values(path, t: pa.Table) -> bool:
    """Check the file holds the table's values.

    The file holds `t`'s values exactly (NaN and -0.0 included), read back in `t`'s
    Arrow types (list items read back as `element`, dictionaries' values as `string`).
    Values, not buffers: the bytes under nulls, and a dictionary's order, are the
    writer's and reader's to choose.
    """
    got = pq.read_table(path).cast(t.schema)
    return got.schema.equals(t.schema, check_metadata=True) and canon(
        got.to_pylist()
    ) == canon(t.to_pylist())


ENGINES = [
    ("native", None),
    ("parallel", None),
    ("parallel", 3),
    ("pyarrow", None),
    ("pyarrow", 4),
]


@pytest.mark.parametrize("engine,workers", ENGINES)
@pytest.mark.parametrize("cdc", [None, True], ids=["columns", "chunked"])
def test_round_trip(tmp_path, cdc, engine, workers):
    """Every engine writes the table's values, in row groups of the size asked."""
    t = table(0, 1000)
    out = tmp_path / "out.parquet"
    n = pq_glue.write_parquet(
        t,
        out,
        engine=engine,
        max_workers=workers,
        row_group_size=300,
        content_defined_chunking=cdc,
    )
    assert n == 1000
    md = pq.read_metadata(out)
    assert [md.row_group(g).num_rows for g in range(md.num_row_groups)] == [
        300,
        300,
        300,
        100,
    ]
    assert values(out, t)
    paths = {md.schema.column(i).path for i in range(md.num_columns)}
    assert "refs.list.element.p" in paths


def test_polars_and_readers(tmp_path):
    """A Polars frame and a record batch reader are written like a table."""
    df = pl.DataFrame({"a": [1, None, 3], "b": [["x"], None, []]})
    pq_glue.write_parquet(df, tmp_path / "df.parquet")
    assert pl.read_parquet(tmp_path / "df.parquet").equals(df)
    t = table(1, 100)
    reader = pa.RecordBatchReader.from_batches(t.schema, t.to_batches(max_chunksize=7))
    pq_glue.write_parquet(reader, tmp_path / "reader.parquet", row_group_size=40)
    assert values(tmp_path / "reader.parquet", t)


@pytest.mark.parametrize("engine,workers", ENGINES)
@pytest.mark.parametrize("cdc", [None, True], ids=["columns", "chunked"])
def test_sorting_columns(tmp_path, cdc, engine, workers):
    """Sorting columns by path, index or tuple are declared in every row group."""
    t = table(2, 200)
    out = tmp_path / "out.parquet"
    pq_glue.write_parquet(
        t,
        out,
        engine=engine,
        max_workers=workers,
        row_group_size=50,
        sorting_columns=["id", (3, True, True)],
        content_defined_chunking=cdc,
    )
    md = pq.read_metadata(out)
    want = (pq.SortingColumn(0), pq.SortingColumn(3, descending=True, nulls_first=True))
    assert all(
        md.row_group(g).sorting_columns == want for g in range(md.num_row_groups)
    )
    with pytest.raises(ValueError, match="no leaf column"):
        pq_glue.write_parquet(t, out, engine=engine, sorting_columns=["nope"])


def test_compression(tmp_path):
    """Each codec and zstd level is written, and an unknown one rejected."""
    t = table(3, 100)
    for name, level, want in [
        ("zstd", None, "ZSTD"),
        ("zstd", 9, "ZSTD"),
        ("snappy", None, "SNAPPY"),
        ("none", None, "UNCOMPRESSED"),
    ]:
        out = tmp_path / f"{name}{level}.parquet"
        pq_glue.write_parquet(t, out, compression=name, compression_level=level)
        assert pq.read_metadata(out).row_group(0).column(0).compression == want
        assert values(out, t)
    with pytest.raises(ValueError, match="compression must be"):
        pq_glue.write_parquet(t, tmp_path / "x.parquet", compression="lzo")
    with pytest.raises(ValueError, match="compression_level is for zstd"):
        pq_glue.write_parquet(
            t, tmp_path / "x.parquet", compression="snappy", compression_level=3
        )


def test_chunking_options(tmp_path):
    """Chunking options are taken, and an unknown one rejected."""
    t = table(4, 500)
    out = tmp_path / "out.parquet"
    options = {"min_chunk_size": 1024, "max_chunk_size": 4096, "norm_level": 1}
    pq_glue.write_parquet(t, out, row_group_size=100, content_defined_chunking=options)
    assert values(out, t)
    with pytest.raises(ValueError, match="no option"):
        pq_glue.write_parquet(t, out, content_defined_chunking={"window": 3})


def test_rejected(tmp_path):
    """Data without an Arrow stream, and empty row groups, are rejected."""
    with pytest.raises(TypeError, match="__arrow_c_stream__"):
        pq_glue.write_parquet([1, 2, 3], tmp_path / "x.parquet")
    with pytest.raises(ValueError, match="row_group_size"):
        pq_glue.write_parquet(table(5, 10), tmp_path / "x.parquet", row_group_size=0)


def test_needs_no_pyarrow(tmp_path):
    """Writing a Polars frame imports no pyarrow."""
    out = tmp_path / "out.parquet"
    code = (
        "import sys, polars as pl, pq_glue; "
        f"pq_glue.write_parquet(pl.DataFrame({{'a': [1, 2]}}), {str(out)!r}); "
        "assert 'pyarrow' not in sys.modules, 'pyarrow imported'"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
    assert pq.read_table(out).num_rows == 2


@pytest.mark.parametrize("cdc", [None, True], ids=["columns", "chunked"])
def test_native_and_parallel_same_bytes(tmp_path, cdc):
    """Check native and parallel are one encoder: the same file at any worker count."""
    t = table(6, 2000)
    files = []
    cases: list[tuple[Literal["native", "parallel"], int | None]] = [
        ("native", None),
        ("parallel", 2),
        ("parallel", 4),
        ("parallel", None),
    ]
    for engine, workers in cases:
        out = tmp_path / f"{engine}-{workers}.parquet"
        pq_glue.write_parquet(
            t,
            out,
            engine=engine,
            max_workers=workers,
            row_group_size=300,
            content_defined_chunking=cdc,
        )
        files.append(out.read_bytes())
    assert all(f == files[0] for f in files)


def test_pyarrow_threads_same_chunks(tmp_path):
    """Check pyarrow on threads writes pyarrow's one-thread column chunks and footer."""
    t = table(7, 1000)
    one, threads = tmp_path / "one.parquet", tmp_path / "threads.parquet"
    pq_glue.write_parquet(
        t,
        one,
        engine="pyarrow",
        row_group_size=128,
        content_defined_chunking=True,
        sorting_columns=["id"],
    )
    pq_glue.write_parquet(
        t,
        threads,
        engine="pyarrow",
        max_workers=4,
        row_group_size=128,
        content_defined_chunking=True,
        sorting_columns=["id"],
    )
    a, b = pq.read_metadata(one), pq.read_metadata(threads)
    assert (a.created_by, a.format_version, a.num_row_groups) == (
        b.created_by,
        b.format_version,
        b.num_row_groups,
    )
    assert [a.row_group(g).sorting_columns for g in range(a.num_row_groups)] == [
        b.row_group(g).sorting_columns for g in range(b.num_row_groups)
    ]

    def chunks(path, md):
        raw = path.read_bytes()
        return [
            raw[
                c.dictionary_page_offset or c.data_page_offset : (
                    c.dictionary_page_offset or c.data_page_offset
                )
                + c.total_compressed_size
            ]
            for g in range(md.num_row_groups)
            for c in (md.row_group(g).column(i) for i in range(md.num_columns))
        ]

    assert chunks(one, a) == chunks(threads, b)
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "one.parquet",
        "threads.parquet",
    ]


def test_engine_options_rejected(tmp_path):
    """An option an engine lacks, or an unknown engine, is rejected."""
    t = table(8, 10)
    out = tmp_path / "x.parquet"
    with pytest.raises(ValueError, match="no adaptive dictionary"):
        pq_glue.write_parquet(t, out, engine="pyarrow", adaptive_dictionary=True)
    with pytest.raises(ValueError, match="one thread"):
        pq_glue.write_parquet(t, out, engine="native", max_workers=2)
    with pytest.raises(ValueError, match="engine must be"):
        pq_glue.write_parquet(t, out, engine="duckdb")  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="max_workers"):
        pq_glue.write_parquet(t, out, max_workers=0)
