"""The compiled extension module (src/python.rs)."""

from collections.abc import Mapping, Sequence
from os import PathLike

__version__: str

def concat(sources: Sequence[str | PathLike[str]], dst: str | PathLike[str]) -> int:
    """Concatenate Parquet files that share one schema into one file.

    Each input's row groups are copied to `dst` as they are encoded (pages,
    dictionaries, statistics), in input order, without being decoded. The output has
    the first input's schema and key-value metadata, and the inputs' column orders,
    page indexes and bloom filters.

    Parameters
    ----------
    sources
        The Parquet files to concatenate, in order. Each must have the first one's
        Parquet schema, key-value metadata and column orders.
    dst
        The file to write, created or truncated. It is removed if the copy fails part
        way, and must not be one of `sources`.

    Returns
    -------
    int
        The number of rows written.

    Raises
    ------
    ValueError
        If there are no sources, `dst` is one of them, a source is not a Parquet file,
        or the sources differ in schema, key-value metadata or column orders.
    OSError
        If a source cannot be opened or `dst` cannot be written.
    """

def write_parquet(
    data: object,
    path: str | PathLike[str],
    *,
    row_group_size: int | None = None,
    compression: str = "zstd",
    compression_level: int | None = None,
    spec_names: bool = True,
    adaptive_dictionary: bool = True,
    sorting_columns: Sequence[str | int | tuple[str | int, bool, bool]] | None = None,
    content_defined_chunking: bool | Mapping[str, int] | None = None,
    max_workers: int | None = None,
) -> int:
    """Write Arrow data with arrow-rs: `pq_glue.write_parquet`'s native engines.

    The "native" and "parallel" engines of `pq_glue.write_parquet` (which see), on
    `max_workers` threads.

    Needs no pyarrow: `data` is read through the Arrow PyCapsule interface. Each row
    group's leaf columns are encoded in parallel; with content-defined chunking, whose
    chunker arrow-rs keeps to its own writer, row groups are encoded in parallel instead,
    each chunked afresh (as pyarrow chunks them), and glued in order.

    Parameters
    ----------
    data
        Any object with an `__arrow_c_stream__` method: a pyarrow table or record batch
        reader, a Polars data frame, ...
    path
        The file to write, created or truncated. It is removed if the write fails.
    row_group_size
        Rows per row group; arrow-rs's default (1,048,576) if None.
    compression
        "zstd" (the default), "snappy" or "none".
    compression_level
        The zstd level; 1 by default.
    spec_names
        List and map groups take the Parquet specification's names (`list`, `element`,
        `key_value`), as pyarrow and Polars write them.
    adaptive_dictionary
        Leaf columns whose sampled values are more than three quarters distinct are not
        dictionary encoded; with False, every leaf column is.
    sorting_columns
        The sorting columns each row group declares: each a leaf column's path (`"id"`,
        `"a.b"`) or index, or a tuple of that, `descending` and `nulls_first`.
    content_defined_chunking
        Content-defined chunking of data pages, for deduplicating storage: True for
        arrow-rs's defaults (256 KiB minimum, 1 MiB maximum chunk, normalisation level
        0, as pyarrow's), or a dict of `min_chunk_size`, `max_chunk_size`, `norm_level`.

    Returns
    -------
    int
        The number of rows written.

    Raises
    ------
    TypeError
        If `data` is not Arrow data of the PyCapsule interface.
    ValueError
        If an option is invalid, or the data cannot be written.
    OSError
        If `path` cannot be written.
    """
