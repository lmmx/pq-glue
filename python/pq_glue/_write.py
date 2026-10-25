"""`write_parquet`: Arrow data written to Parquet by one of several engines."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from os import PathLike
from pathlib import Path
from typing import Literal

from . import _pq_glue

Engine = Literal["native", "parallel", "pyarrow"]
SortingColumn = str | int | tuple[str | int, bool, bool]


def write_parquet(
    data: object,
    path: str | PathLike[str],
    *,
    engine: Engine = "parallel",
    max_workers: int | None = None,
    row_group_size: int | None = None,
    compression: str = "zstd",
    compression_level: int | None = None,
    sorting_columns: Sequence[SortingColumn] | None = None,
    statistics_columns: Sequence[str | int] | None = None,
    page_index: bool = True,
    content_defined_chunking: bool | Mapping[str, int] | None = None,
    spec_names: bool = True,
    adaptive_dictionary: bool | None = None,
) -> int:
    """Write Arrow data to a Parquet file with the engine asked for; returns the rows written.

    Every engine writes the same values, in the same order, with the same Arrow schema,
    row groups, compression, sorting columns, statistics, page index and content-defined
    chunking. Their bytes
    differ (page boundaries, dictionaries, the footer's `created_by` and format
    version): see the documentation's engines page. An option an engine lacks raises
    `ValueError`.

    Parameters
    ----------
    data
        Any object with an `__arrow_c_stream__` method: a pyarrow table or record batch
        reader, a Polars data frame, ...
    path
        The file to write, created or truncated.
    engine
        - "native": arrow-rs, polars-genson's writer, on one thread.
        - "parallel": the same writer, on `max_workers` threads (by default one per
          CPU): each row group's leaf columns in parallel, or, with content-defined
          chunking, whole row groups in parallel, glued in order. The same bytes as
          "native", sooner, holding a row group in flight per thread.
        - "pyarrow": pyarrow's writer (pyarrow needed: `pq-glue[pyarrow]`), on one
          thread, or with `max_workers` above 1 a row group per thread, glued in order.
    max_workers
        Threads that encode at once; by default 1 for "native" and "pyarrow", one per
        CPU for "parallel".
    row_group_size
        Rows per row group; 1,048,576 if None.
    compression
        "zstd" (the default), "snappy" or "none".
    compression_level
        The zstd level; 1 if None.
    sorting_columns
        The sorting columns each row group declares: each a leaf column's path (`"id"`,
        `"a.b"`) or index, or a tuple of that, `descending` and `nulls_first`. The rows
        must already be in that order.
    statistics_columns
        The leaf columns given statistics, each a path or index as for
        `sorting_columns`: each column chunk's min, max and null count, and with
        `page_index` each page's. The other leaf columns have none, which spares the
        writer a comparison per value for columns no reader filters on. Every leaf
        column if None.
    page_index
        Write the page index: every column chunk's page locations (the offset index),
        and the statistics columns' page statistics (the column index). Without it,
        the statistics columns have column chunk statistics alone.
    content_defined_chunking
        Content-defined chunking of data pages, for deduplicating storage: True for the
        defaults (256 KiB minimum, 1 MiB maximum chunk, normalisation level 0), or a
        dict of `min_chunk_size`, `max_chunk_size`, `norm_level`. Chunking starts afresh
        at each row group in every engine.
    spec_names
        List and map groups take the Parquet specification's names (`list`, `element`,
        `key_value`).
    adaptive_dictionary
        ("native", "parallel") Leaf columns whose sampled values are more than three
        quarters distinct are not dictionary encoded; True if None. "pyarrow" encodes
        every leaf with a dictionary, and rejects True.

    Returns
    -------
    int
        The number of rows written.
    """
    if max_workers is not None and max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if row_group_size is not None and row_group_size < 1:
        raise ValueError("row_group_size must be at least 1")
    if engine == "native":
        if max_workers not in (None, 1):
            raise ValueError(
                'engine "native" runs on one thread: use engine "parallel" for max_workers'
            )
        workers = 1
    elif engine == "parallel":
        workers = max_workers
    elif engine == "pyarrow":
        if adaptive_dictionary:
            raise ValueError('engine "pyarrow" has no adaptive dictionary')
        return _pyarrow(
            data,
            Path(path),
            max_workers or 1,
            row_group_size=row_group_size,
            compression=compression,
            compression_level=compression_level,
            sorting_columns=sorting_columns,
            statistics_columns=statistics_columns,
            page_index=page_index,
            content_defined_chunking=content_defined_chunking,
            spec_names=spec_names,
        )
    else:
        raise ValueError(
            f'engine must be "native", "parallel" or "pyarrow", not {engine!r}'
        )
    return _pq_glue.write_parquet(
        data,
        path,
        max_workers=workers,
        adaptive_dictionary=True
        if adaptive_dictionary is None
        else adaptive_dictionary,
        row_group_size=row_group_size,
        compression=compression,
        compression_level=compression_level,
        sorting_columns=sorting_columns,
        statistics_columns=statistics_columns,
        page_index=page_index,
        content_defined_chunking=content_defined_chunking,
        spec_names=spec_names,
    )


def _pyarrow(
    data: object,
    path: Path,
    workers: int,
    *,
    row_group_size: int | None,
    compression: str,
    compression_level: int | None,
    sorting_columns: Sequence[SortingColumn] | None,
    statistics_columns: Sequence[str | int] | None,
    page_index: bool,
    content_defined_chunking: bool | Mapping[str, int] | None,
    spec_names: bool,
) -> int:
    """Write with pyarrow, with the native engines' defaults.

    Those are zstd level 1, a page index, statistics for every leaf column and row
    groups of 1,048,576 rows.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        raise ImportError(
            'engine "pyarrow" needs pyarrow: pip install "pq-glue[pyarrow]"'
        ) from e
    table = pa.RecordBatchReader.from_stream(data).read_all()
    codec = {
        "zstd": "zstd",
        "snappy": "snappy",
        "none": "none",
        "uncompressed": "none",
    }.get(compression)
    if codec is None:
        raise ValueError(
            f"compression must be 'zstd', 'snappy' or 'none', not {compression!r}"
        )
    if compression_level is not None and codec != "zstd":
        raise ValueError(f"compression_level is for zstd, not {compression}")
    named = sorting_columns or statistics_columns is not None
    paths = _leaf_paths(table.schema, spec_names) if named else []
    options: dict = dict(
        compression=codec,
        write_page_index=page_index,
        write_statistics=True
        if statistics_columns is None
        else [paths[_leaf(paths, c)] for c in statistics_columns],
        use_compliant_nested_type=spec_names,
        sorting_columns=_sorting(paths, sorting_columns) if sorting_columns else None,
        use_content_defined_chunking=dict(content_defined_chunking)
        if isinstance(content_defined_chunking, Mapping)
        else bool(content_defined_chunking),
    )
    if codec == "zstd":
        options["compression_level"] = (
            1 if compression_level is None else compression_level
        )
    rows = row_group_size or 1024 * 1024
    groups = [table.slice(i, rows) for i in range(0, table.num_rows, rows)]

    def write(dst: Path, parts) -> None:
        with pq.ParquetWriter(dst, table.schema, **options) as writer:
            for part in parts:
                writer.write_table(part, row_group_size=max(part.num_rows, 1))

    if workers <= 1 or len(groups) <= 1:
        write(path, groups)
        return table.num_rows
    parts_dir = Path(tempfile.mkdtemp(prefix=f".{path.name}.", dir=path.parent))
    try:
        parts = [parts_dir / f"{i}.parquet" for i in range(len(groups))]
        with ThreadPoolExecutor(min(workers, len(groups), os.cpu_count() or 1)) as pool:
            list(pool.map(lambda i: write(parts[i], [groups[i]]), range(len(groups))))
        return _pq_glue.concat(parts, path)
    finally:
        shutil.rmtree(parts_dir, ignore_errors=True)


def _leaf_paths(schema, spec_names: bool) -> list[str]:
    """The paths of the Parquet leaf columns `schema` is written as."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    sink = pa.BufferOutputStream()
    pq.write_table(schema.empty_table(), sink, use_compliant_nested_type=spec_names)
    meta = pq.read_metadata(pa.BufferReader(sink.getvalue()))
    return [meta.schema.column(i).path for i in range(meta.num_columns)]


def _leaf(paths: list[str], column: str | int) -> int:
    """The index of the leaf column `column` names, as the native engines resolve it."""
    if isinstance(column, int):
        if not 0 <= column < len(paths):
            raise ValueError(f"no leaf column {column}")
        return column
    if column in paths:
        return paths.index(column)
    raise ValueError(f"no leaf column {column!r}")


def _sorting(paths: list[str], columns: Sequence[SortingColumn]) -> list:
    """Convert `columns` to pyarrow's sorting columns."""
    import pyarrow.parquet as pq

    out = []
    for column in columns:
        column, descending, nulls_first = (
            column if isinstance(column, tuple) else (column, False, False)
        )
        out.append(
            pq.SortingColumn(
                _leaf(paths, column), descending=descending, nulls_first=nulls_first
            )
        )
    return out
