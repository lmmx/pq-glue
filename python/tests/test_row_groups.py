"""concat and concat_many of chosen row groups: copied as whole files are copied."""

import pq_glue
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from .conftest import write


def test_every_row_group_in_order_is_the_whole_file(tmp_path):
    """A file's row groups listed in order copy as the file itself."""
    srcs = [write(tmp_path, i, 90 + i) for i in range(2)]
    whole, picked = tmp_path / "whole.parquet", tmp_path / "picked.parquet"
    rows = pq_glue.concat(srcs, whole)
    chosen = [(s, list(range(pq.read_metadata(s).num_row_groups))) for s in srcs]
    assert pq_glue.concat(chosen, picked) == rows
    assert picked.read_bytes() == whole.read_bytes()


def test_chosen_row_groups_in_order_chosen(tmp_path):
    """Only the chosen row groups are copied, in the order chosen."""
    src = write(tmp_path, 0, 160)  # 4 row groups of 40 rows
    dst = tmp_path / "out.parquet"
    assert pq_glue.concat([(src, [2, 0])], dst) == 80
    f = pq.ParquetFile(src)
    want = pa.concat_tables([f.read_row_group(2), f.read_row_group(0)])
    assert pq.read_table(dst).equals(want)


def test_split_and_glued_back(tmp_path):
    """Each row group copied out alone, then those glued, is the file glued whole."""
    src = write(tmp_path, 0, 130)
    whole = tmp_path / "whole.parquet"
    pq_glue.concat([src], whole)
    n = pq.read_metadata(src).num_row_groups
    jobs = [([(src, [r])], tmp_path / f"part_{r}.parquet") for r in range(n)]
    assert sum(pq_glue.concat_many(jobs)) == 130
    again = tmp_path / "again.parquet"
    pq_glue.concat([dst for _, dst in jobs], again)
    assert again.read_bytes() == whole.read_bytes()


def test_jobs_sharing_a_file_write_what_each_writes_alone(tmp_path):
    """Jobs taking row groups of the same files write what concat writes for each."""
    a, b = write(tmp_path, 0, 200), write(tmp_path, 1, 120)
    jobs = [([(a, [j, 4 - j]), (b, [j % 3])], tmp_path / f"job_{j}.parquet") for j in range(5)]
    rows = pq_glue.concat_many(jobs)
    for (sources, dst), n in zip(jobs, rows):
        alone = dst.with_suffix(".alone.parquet")
        assert pq_glue.concat(sources, alone) == n
        assert dst.read_bytes() == alone.read_bytes()


def test_a_row_group_not_in_the_file(tmp_path):
    """A row group the file lacks raises ValueError, naming the file, and writes nothing."""
    src = write(tmp_path, 0, 50)
    dst = tmp_path / "out.parquet"
    with pytest.raises(ValueError, match="no row group 7"):
        pq_glue.concat([(src, [0, 7])], dst)
    assert not dst.exists()
