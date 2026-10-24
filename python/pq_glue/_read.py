"""`read_parquet`: a Parquet file read by pq-glue's reader (arrow-rs), row groups in parallel."""

from __future__ import annotations

from collections.abc import Sequence
from os import PathLike

from . import _pq_glue


class _Stream:
    """An Arrow C stream capsule behind the Arrow PyCapsule interface, read once."""

    def __init__(self, capsule: object) -> None:
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return self._capsule


def read_parquet(path: str | PathLike[str], row_groups: Sequence[int] | None = None):
    """Read a Parquet file to a `pyarrow.Table`, each row group decoded in parallel.

    The table has the file's Arrow schema (from its `ARROW:schema` metadata) and one
    chunk per row group read, in the order of `row_groups` (all of them, in order, if
    None). The values are those `pyarrow.parquet.ParquetFile.read` and `read_row_groups`
    give; the file is read into memory once, then decoded by arrow-rs on every core,
    without the GIL.

    Raises
    ------
    ValueError
        If the file is not Parquet, or a row group asked for is not in it.
    OSError
        If the file cannot be read.
    """
    import pyarrow as pa

    groups = None if row_groups is None else [int(g) for g in row_groups]
    return pa.table(_Stream(_pq_glue.read_parquet(path, groups)))
