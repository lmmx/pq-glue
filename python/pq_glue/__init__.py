"""Concatenate Parquet files by copying their encoded row groups, without decoding them,
and write Arrow data to Parquet, encoding in parallel (`write_parquet`)."""

from ._pq_glue import __version__, concat, write_parquet

__all__ = ["__version__", "concat", "write_parquet"]
