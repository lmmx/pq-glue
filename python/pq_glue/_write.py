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
    content_defined_chunking: bool | Mapping[str, int] | None = None,
    spec_names: bool = True,
    adaptive_dictionary: bool | None = None,
    column_encoding: Mapping[str, str] | None = None,
) -> int:
    """Write Arrow data to a Parquet file with the engine asked for; returns the rows written.

    Every engine writes the same values, in the same order, with the same Arrow schema,
    row groups, compression, sorting columns and content-defined chunking. Their bytes
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
    column_encoding
        Leaf columns written in an encoding of their own, with no dictionary: a dict of
        each leaf's path in the Parquet schema (`"id"`, `"a.b"`, `"xs.list.element"`)
        to its encoding's name, e.g. `{"statement_id": "DELTA_BYTE_ARRAY"}` for strings
        that share a prefix with the value before. Only the file's bytes change: every
        engine reads back the same values, schema and statistics. The encodings:
        "PLAIN", "RLE" (booleans), "DELTA_BINARY_PACKED" (32 and 64-bit integers),
        "DELTA_LENGTH_BYTE_ARRAY" (strings and binary), "DELTA_BYTE_ARRAY" (strings and
        binary, fixed-length too), "BYTE_STREAM_SPLIT" (floats, integers, fixed-length
        binary); "native" and "parallel" also take "ALP" (floats). A dictionary is
        `adaptive_dictionary`'s to decide. An unknown column, or an encoding its type
        does not take, raises `ValueError`.

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
            content_defined_chunking=content_defined_chunking,
            spec_names=spec_names,
            column_encoding=column_encoding,
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
        content_defined_chunking=content_defined_chunking,
        spec_names=spec_names,
        column_encoding=dict(column_encoding) if column_encoding else None,
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
    content_defined_chunking: bool | Mapping[str, int] | None,
    spec_names: bool,
    column_encoding: Mapping[str, str] | None = None,
) -> int:
    """Write with pyarrow, with the native engines' defaults.

    Those are zstd level 1, a page index and row groups of 1,048,576 rows.
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
    options: dict = dict(
        compression=codec,
        write_page_index=True,
        use_compliant_nested_type=spec_names,
        sorting_columns=_sorting(table.schema, sorting_columns, spec_names)
        if sorting_columns
        else None,
        use_content_defined_chunking=dict(content_defined_chunking)
        if isinstance(content_defined_chunking, Mapping)
        else bool(content_defined_chunking),
    )
    if column_encoding:
        leaves = _leaf_types(table.schema, spec_names)
        encodings = {
            c: _check_encoding(c, e, leaves) for c, e in column_encoding.items()
        }
        # pyarrow takes an encoding only for a column with no dictionary
        options["use_dictionary"] = [p for p in leaves if p not in encodings]
        options["column_encoding"] = encodings
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


def _sorting(schema, columns: Sequence[SortingColumn], spec_names: bool) -> list:
    """Convert `columns` to pyarrow's sorting columns.

    Leaf paths are resolved as the native engines resolve them.
    """
    import pyarrow.parquet as pq

    paths = list(_leaf_types(schema, spec_names))
    out = []
    for column in columns:
        column, descending, nulls_first = (
            column if isinstance(column, tuple) else (column, False, False)
        )
        if isinstance(column, int):
            if not 0 <= column < len(paths):
                raise ValueError(f"no leaf column {column}")
            index = column
        elif column in paths:
            index = paths.index(column)
        else:
            raise ValueError(f"no leaf column {column!r}")
        out.append(
            pq.SortingColumn(index, descending=descending, nulls_first=nulls_first)
        )
    return out


def _leaf_types(schema, spec_names: bool) -> dict[str, str]:
    """Each Parquet leaf column's physical type, by its path, as pyarrow writes `schema`."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    sink = pa.BufferOutputStream()
    pq.write_table(schema.empty_table(), sink, use_compliant_nested_type=spec_names)
    meta = pq.read_metadata(pa.BufferReader(sink.getvalue()))
    return {
        meta.schema.column(i).path: meta.schema.column(i).physical_type
        for i in range(meta.num_columns)
    }


# The physical types each encoding takes, as the native engines check them (pq-glue's
# write::check_encoding)
_ENCODES = {
    "PLAIN": None,
    "RLE": {"BOOLEAN"},
    "DELTA_BINARY_PACKED": {"INT32", "INT64"},
    "DELTA_LENGTH_BYTE_ARRAY": {"BYTE_ARRAY"},
    "DELTA_BYTE_ARRAY": {"BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"},
    "BYTE_STREAM_SPLIT": {"FLOAT", "DOUBLE", "INT32", "INT64", "FIXED_LEN_BYTE_ARRAY"},
}


def _check_encoding(column: str, encoding: str, leaves: dict[str, str]) -> str:
    """The encoding's name for pyarrow, checked as the native engines check it.

    Raises ValueError, as they do, for a column `leaves` lacks, an unknown encoding or a
    dictionary's, or one the column's physical type does not take.
    """
    name = encoding.upper()
    if column not in leaves:
        raise ValueError(f"no leaf column {column!r} to encode")
    if name in ("PLAIN_DICTIONARY", "RLE_DICTIONARY", "BIT_PACKED"):
        raise ValueError(
            f"{column!r}: {name} is not an encoding to ask for "
            "(a dictionary is adaptive_dictionary's)"
        )
    if name not in _ENCODES:
        raise ValueError(f"{column!r}: no encoding {encoding!r}")
    types = _ENCODES[name]
    if types is not None and leaves[column] not in types:
        raise ValueError(f"{column!r}: {name} does not encode {leaves[column]} values")
    return name
