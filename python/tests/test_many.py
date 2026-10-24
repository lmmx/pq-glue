"""concat_many: each job's output is concat's, and a failed job fails alone."""

from pathlib import Path

import pq_glue
import pyarrow.parquet as pq
import pytest
from .conftest import table, write


def jobs(tmp_path, n: int) -> list[tuple[list[str], Path]]:
    """`n` jobs of one to four pyarrow-written inputs each, in a directory each."""
    out = []
    for j in range(n):
        sub = tmp_path / f"job_{j}"
        sub.mkdir()
        sources = [
            write(sub, i, 1 + (j * 37 + i * 11) % 120, row_group_size=16 + j % 3 * 8)
            for i in range(1 + j % 4)
        ]
        out.append((sources, sub / "out.parquet"))
    return out


def test_outputs_are_concats(tmp_path):
    """Each job's output is, byte for byte, what concat writes for it."""
    many = jobs(tmp_path, 24)
    rows = pq_glue.concat_many(many)
    assert rows == [sum(pq.read_metadata(s).num_rows for s in srcs) for srcs, _ in many]
    for (sources, dst), n in zip(many, rows):
        alone = dst.with_suffix(".alone.parquet")
        assert pq_glue.concat(sources, alone) == n
        assert dst.read_bytes() == alone.read_bytes()


def test_no_jobs():
    """No jobs, no rows."""
    assert pq_glue.concat_many([]) == []


def test_error_names_the_destination(tmp_path):
    """A failed job raises as concat does, naming its dst; the other jobs are written."""
    many = jobs(tmp_path, 4)
    other = tmp_path / "other.parquet"
    pq.write_table(table(0, 10).drop_columns(["n"]), other)
    many[2][0].append(str(other))
    with pytest.raises(ValueError, match="job_2.*not written: .*other schema"):
        pq_glue.concat_many(many)
    assert [dst.exists() for _, dst in many] == [True, True, False, True]


def test_missing_source_raises_oserror(tmp_path):
    """A job with a source that cannot be opened raises OSError, naming its dst."""
    many = jobs(tmp_path, 3)
    many[1][0].append(str(tmp_path / "missing.parquet"))
    with pytest.raises(OSError, match="job_1"):
        pq_glue.concat_many(many)


def test_first_failed_job_raises(tmp_path):
    """Of several failed jobs, the first in the jobs' order is raised."""
    many = jobs(tmp_path, 4)
    for j in (1, 3):
        many[j][0].append(str(tmp_path / "missing.parquet"))
    with pytest.raises(OSError, match="job_1"):
        pq_glue.concat_many(many)
