"""Inputs written with pyarrow, and comparisons of the output with them."""

import math
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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


@pytest.fixture
def inputs(tmp_path):
    """Four pyarrow-written files of 100, 37, 1 and 250 rows, in row groups of 40."""
    return [write(tmp_path, i, rows) for i, rows in enumerate([100, 37, 1, 250])]
