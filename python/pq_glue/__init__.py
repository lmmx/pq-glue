"""Glue Parquet files together, and write Arrow data to Parquet and read it back.

`concat` copies encoded row groups without decoding them (`concat_many` for many
concatenations at once, `concat_size` for the size of its file without writing it); `write_parquet` writes Arrow data with a choice of engines;
`read_parquet` reads a file's row groups in parallel.
"""

from ._pq_glue import __version__, concat, concat_many, concat_size
from ._read import read_parquet
from ._write import write_parquet

__all__ = ["__version__", "concat", "concat_many", "concat_size", "read_parquet", "write_parquet"]
