"""The output's footer: schema, metadata, row groups, statistics and page indexes."""

import struct

import pq_glue
import pyarrow as pa
import pyarrow.parquet as pq
from .conftest import float_chunks, row_group_stats, same, write


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
