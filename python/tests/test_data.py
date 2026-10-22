"""The output holds the inputs' rows, in order."""

import polars as pl
import pq_glue
import pyarrow as pa
import pyarrow.parquet as pq
from .conftest import same, table, write


def test_rows_and_data(inputs, tmp_path):
    """The output has the inputs' rows, in order, as pyarrow and Polars read them."""
    out = tmp_path / "out.parquet"
    assert pq_glue.concat(inputs, out) == 388
    want = pa.concat_tables(pq.read_table(p) for p in inputs)
    assert same(pq.read_table(out), want)
    assert pl.read_parquet(out).equals(
        pl.concat([pl.read_parquet(p) for p in inputs]), null_equal=True
    )


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
