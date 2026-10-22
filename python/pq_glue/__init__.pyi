from os import PathLike
from collections.abc import Sequence

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
