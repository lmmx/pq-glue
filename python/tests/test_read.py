"""read_parquet reads the values pyarrow reads, row groups in parallel and in order."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import pq_glue


@pytest.fixture
def nested(tmp_path):
    """A file of nested columns (lists of structs, a map, nulls) in 8 row groups."""
    n = 1000
    table = pa.table(
        {
            "id": [f"Q{i}" for i in range(n)],
            "refs": [
                [{"p": f"P{j}", "v": [j, None]} for j in range(i % 4)] for i in range(n)
            ],
            "m": pa.array(
                [[("en", f"l{i}")] if i % 3 else None for i in range(n)],
                pa.map_(pa.string(), pa.string()),
            ),
            "x": [None if i % 7 == 0 else i * 0.5 for i in range(n)],
        }
    )
    path = tmp_path / "nested.parquet"
    pq.write_table(table, path, row_group_size=128, compression="zstd")
    return path, table


def test_whole_file_is_pyarrows_read(nested):
    """The whole file reads as pyarrow reads it, a chunk per row group."""
    path, table = nested
    got = pq_glue.read_parquet(path)
    want = pq.ParquetFile(path).read()
    assert got.schema.remove_metadata() == want.schema.remove_metadata()
    assert got.equals(want)
    assert got.equals(table)
    assert [len(c) for c in got.column(0).chunks] == [128] * 7 + [104]


def test_row_groups_in_the_order_asked(nested):
    """Row groups asked for come back in the order asked."""
    path, _ = nested
    f = pq.ParquetFile(path)
    got = pq_glue.read_parquet(path, row_groups=[3, 0, 7])
    want = pa.concat_tables([f.read_row_group(g) for g in (3, 0, 7)])
    assert got.equals(want)


def test_large_types_kept(tmp_path):
    """Large string and list types come back as stored in ARROW:schema."""
    table = pa.table(
        {
            "s": pa.array(["a", None], pa.large_string()),
            "l": pa.array([[1], []], pa.large_list(pa.int64())),
        }
    )
    path = tmp_path / "large.parquet"
    pq.write_table(table, path)
    assert pq_glue.read_parquet(path).schema.remove_metadata() == table.schema


def test_errors(nested, tmp_path):
    """A missing row group, a non-Parquet file and a missing file raise."""
    path, _ = nested
    with pytest.raises(ValueError, match="no row group 8"):
        pq_glue.read_parquet(path, row_groups=[8])
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not parquet")
    with pytest.raises(ValueError):
        pq_glue.read_parquet(bad)
    with pytest.raises(OSError):
        pq_glue.read_parquet(tmp_path / "missing.parquet")
