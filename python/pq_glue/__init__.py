"""Concatenate Parquet files by copying their encoded row groups, without decoding them."""

from ._pq_glue import __version__, concat

__all__ = ["__version__", "concat"]
