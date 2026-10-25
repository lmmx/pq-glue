"""concat_size: the size of the file concat writes, without writing it."""

import pq_glue
import pyarrow.parquet as pq
import pytest
from .conftest import table, write


@pytest.mark.parametrize("n", [1, 2, 3, 5])
def test_the_size_of_concats_file(tmp_path, n):
    """pyarrow-written inputs (dictionary pages, no page index): concat's file's size."""
    sources = [write(tmp_path, i, 40 + 23 * i, row_group_size=16 + 8 * (i % 3)) for i in range(n)]
    dst = tmp_path / "out.parquet"
    pq_glue.concat(sources, dst)
    assert pq_glue.concat_size(sources) == dst.stat().st_size


def test_the_size_with_page_indexes(tmp_path):
    """Inputs with page indexes, of more row groups than one varint byte counts."""
    sources = []
    for i in range(3):
        path = tmp_path / f"pi_{i}.parquet"
        pq.write_table(table(i, 3_000), path, row_group_size=40, write_page_index=True)
        sources.append(str(path))
    dst = tmp_path / "out.parquet"
    pq_glue.concat(sources, dst)
    assert pq.read_metadata(dst).num_row_groups > 64
    assert pq_glue.concat_size(sources) == dst.stat().st_size


def test_errors_as_concats(tmp_path):
    """No sources and differing schemas raise ValueError, a missing source OSError."""
    a = write(tmp_path, 0, 10)
    other = tmp_path / "other.parquet"
    pq.write_table(table(0, 10).drop_columns(["n"]), other)
    with pytest.raises(ValueError):
        pq_glue.concat_size([])
    with pytest.raises(ValueError, match="other schema"):
        pq_glue.concat_size([a, other])
    with pytest.raises(OSError):
        pq_glue.concat_size([a, tmp_path / "missing.parquet"])
