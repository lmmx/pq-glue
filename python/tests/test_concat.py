"""pq_glue.concat against its inputs, written with pyarrow."""

import math
import struct

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import pq_glue

SCHEMA = pa.schema(
    [
        pa.field("id", pa.large_string(), nullable=False),
        pa.field("x", pa.float64()),
        pa.field("f", pa.float32()),
        pa.field("n", pa.int64()),
        pa.field("tag", pa.dictionary(pa.int32(), pa.large_string())),
        pa.field(
            "refs",
            pa.large_list(
                pa.struct(
                    [
                        pa.field("p", pa.large_string()),
                        pa.field("v", pa.large_list(pa.float64())),
                    ]
                )
            ),
        ),
    ],
    metadata={"origin": "test"},
)

SPECIAL = [math.nan, -0.0, 0.0, None, 1.5, -2.25, math.inf, -math.inf]


def table(i: int, rows: int) -> pa.Table:
    """Rows of file `i`, differing between files.

    They have nulls, empty and null lists, and floats with NaN, signed zeros and
    infinities.
    """
    xs = [
        SPECIAL[(r + i) % len(SPECIAL)] if r % 3 else r * 0.5 + i for r in range(rows)
    ]
    refs = []
    for r in range(rows):
        if r % 7 == 0:
            refs.append(None)
        elif r % 5 == 0:
            refs.append([])
        else:
            refs.append(
                [{"p": f"P{r % 11}", "v": [xs[r], None, -0.0]}, {"p": None, "v": None}]
            )
    return pa.table(
        {
            "id": [f"Q{i}-{r:05d}" for r in range(rows)],
            "x": xs,
            "f": [None if r % 4 == 0 else float(r % 9) - 4.0 for r in range(rows)],
            "n": [None if r % 6 == 0 else r - 50 for r in range(rows)],
            "tag": pa.array([["a", "b", None][r % 3] for r in range(rows)])
            .dictionary_encode()
            .cast(SCHEMA.field("tag").type),
            "refs": refs,
        },
        schema=SCHEMA,
    )


def same(a: pa.Table, b: pa.Table) -> bool:
    """Equal schemas (metadata included) and bit-identical values.

    Compared through Arrow IPC: Table.equals has NaN unequal to itself.
    """

    def ipc(t: pa.Table) -> bytes:
        """The table as one Arrow IPC stream."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, t.schema) as w:
            w.write_table(t.combine_chunks())
        return sink.getvalue().to_pybytes()

    return a.schema.equals(b.schema, check_metadata=True) and ipc(a) == ipc(b)


def write(tmp_path, i: int, rows: int, **kw) -> str:
    """Write `table(i, rows)` to a file in `tmp_path`, in row groups of 40 rows by default."""
    path = tmp_path / f"in_{i}.parquet"
    pq.write_table(
        table(i, rows), path, row_group_size=kw.pop("row_group_size", 40), **kw
    )
    return str(path)


def row_group_stats(paths):
    """Each row group's columns' statistics, as pyarrow reads them (NaN-safe tuples)."""
    out = []
    for path in paths:
        md = pq.read_metadata(path)
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            for c in range(rg.num_columns):
                s = rg.column(c).statistics
                if s is None:
                    out.append((rg.column(c).path_in_schema, None))
                    continue
                pack = (
                    lambda v: struct.pack("<d", v) if isinstance(v, float) else v
                )  # -0.0 != 0.0
                mm = (pack(s.min), pack(s.max)) if s.has_min_max else None
                out.append(
                    (
                        rg.column(c).path_in_schema,
                        s.has_min_max,
                        mm,
                        s.null_count,
                        s.num_values,
                    )
                )
            out.append(("rows", rg.num_rows))
    return out


@pytest.fixture
def inputs(tmp_path):
    """Four pyarrow-written files of 100, 37, 1 and 250 rows, in row groups of 40."""
    return [write(tmp_path, i, rows) for i, rows in enumerate([100, 37, 1, 250])]


def test_rows_and_data(inputs, tmp_path):
    """The output has the inputs' rows, in order, as pyarrow and Polars read them."""
    out = tmp_path / "out.parquet"
    assert pq_glue.concat(inputs, out) == 388
    want = pa.concat_tables(pq.read_table(p) for p in inputs)
    assert same(pq.read_table(out), want)
    assert pl.read_parquet(out).equals(
        pl.concat([pl.read_parquet(p) for p in inputs]), null_equal=True
    )


def test_footer_matches_inputs(inputs, tmp_path):
    """The output's schema, metadata, row groups and statistics are the inputs'."""
    out = tmp_path / "out.parquet"
    pq_glue.concat(inputs, out)
    assert pq.read_schema(out).equals(pq.read_schema(inputs[0]), check_metadata=True)
    assert pq.read_metadata(out).metadata == pq.read_metadata(inputs[0]).metadata
    assert pq.read_metadata(out).num_row_groups == sum(
        pq.read_metadata(p).num_row_groups for p in inputs
    )
    assert row_group_stats([out]) == row_group_stats(inputs)


def float_chunks(path):
    """Each float column chunk's (column, has min/max, min, max).

    Min and max are packed to bytes, so that -0.0 and 0.0 differ and NaN equals itself.
    """
    md = pq.read_metadata(path)
    pack = lambda v: struct.pack("<d", v)
    out = []
    for g in range(md.num_row_groups):
        for c in range(md.num_columns):
            col = md.row_group(g).column(c)
            if col.physical_type in ("DOUBLE", "FLOAT"):
                st = col.statistics
                out.append(
                    (
                        col.path_in_schema,
                        st.has_min_max,
                        *((pack(st.min), pack(st.max)) if st.has_min_max else ()),
                    )
                )
    return out


def test_float_statistics_visible(inputs, tmp_path):
    """The float columns' min/max, signed zeros included, are the inputs' in pyarrow.

    parquet-rs alone would mark the columns IEEE 754 ordered, and Arrow C++ would
    ignore every one.
    """
    out = tmp_path / "out.parquet"
    pq_glue.concat(inputs, out)
    want = [chunk for p in inputs for chunk in float_chunks(p)]
    got = float_chunks(out)
    assert got == want
    assert sum(c[1] for c in got) > 0, "no float statistics to check"
    assert any(c[2] == struct.pack("<d", -0.0) for c in got if c[1]), (
        "no -0.0 min to check"
    )


def test_page_index(tmp_path):
    """The inputs' page indexes are copied: reads filtered through them match."""
    inputs = [
        write(tmp_path, i, 120, write_page_index=True, data_page_size=64)
        for i in range(3)
    ]
    out = tmp_path / "out.parquet"
    pq_glue.concat(inputs, out)
    # Read with the page index, filtering on it, and compare with the inputs'
    want = pa.concat_tables(pq.read_table(p, filters=[("n", ">", 10)]) for p in inputs)
    assert same(pq.read_table(out, filters=[("n", ">", 10)]), want)
    assert pq.ParquetFile(out).metadata.row_group(0).column(0).has_offset_index


def test_single_input(tmp_path):
    """One input is copied whole."""
    src = write(tmp_path, 0, 90)
    out = tmp_path / "out.parquet"
    assert pq_glue.concat([src], out) == 90
    assert same(pq.read_table(out), pq.read_table(src))


def test_empty_file_input(tmp_path):
    """An input with no row groups adds nothing."""
    empty = tmp_path / "empty.parquet"
    pq.write_table(table(0, 0), empty)
    src = write(tmp_path, 1, 30)
    out = tmp_path / "out.parquet"
    assert pq_glue.concat([str(empty), src], out) == 30
    assert same(pq.read_table(out), pq.read_table(src))


def test_other_schema_rejected(tmp_path, inputs):
    """An input with another schema is rejected, and no output is left."""
    other = tmp_path / "other.parquet"
    pq.write_table(table(0, 10).drop_columns(["n"]), other)
    out = tmp_path / "out.parquet"
    with pytest.raises(ValueError, match="other schema"):
        pq_glue.concat([*inputs, str(other)], out)
    assert not out.exists()


def test_other_metadata_rejected(tmp_path, inputs):
    """An input with other key-value metadata is rejected."""
    other = tmp_path / "other.parquet"
    pq.write_table(table(0, 10).replace_schema_metadata({"origin": "elsewhere"}), other)
    with pytest.raises(ValueError, match="key-value metadata"):
        pq_glue.concat([inputs[0], str(other)], tmp_path / "out.parquet")


def test_no_inputs(tmp_path):
    """No inputs is an error."""
    with pytest.raises(ValueError, match="no input"):
        pq_glue.concat([], tmp_path / "out.parquet")


def test_output_is_input(inputs):
    """The output may not be one of the inputs."""
    with pytest.raises(ValueError, match="one of the inputs"):
        pq_glue.concat(inputs, inputs[1])


def test_missing_input(tmp_path, inputs):
    """An input that cannot be opened raises OSError."""
    with pytest.raises(OSError):
        pq_glue.concat(
            [*inputs, str(tmp_path / "missing.parquet")], tmp_path / "out.parquet"
        )


def test_not_parquet(tmp_path, inputs):
    """An input that is not Parquet raises ValueError."""
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not parquet at all")
    with pytest.raises(ValueError):
        pq_glue.concat([inputs[0], str(bad)], tmp_path / "out.parquet")
