"""Glue Parquet files together, and write Arrow data to Parquet.

`concat` copies encoded row groups without decoding them; `write_parquet` writes Arrow
data with a choice of engines.
"""

from ._pq_glue import __version__, concat
from ._write import write_parquet

__all__ = ["__version__", "concat", "write_parquet"]
