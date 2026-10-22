from os import PathLike
from collections.abc import Sequence

__version__: str

def concat(sources: Sequence[str | PathLike[str]], dst: str | PathLike[str]) -> int:
    """Concatenate Parquet files with one schema into `dst`, copying their encoded row
    groups without decoding them; returns the rows written. Raises ValueError if the
    files differ in schema or key-value metadata, OSError if one cannot be read."""
