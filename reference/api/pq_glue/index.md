# pq\_glue

Concatenate Parquet files by copying their encoded row groups, without decoding them.

## \_\_version\_\_

```python
__version__: str
```

## concat

```python
concat(sources: Sequence[str | PathLike[str]], dst: str | PathLike[str]) -> int
```

Concatenate Parquet files that share one schema into one file.

Each input's row groups are copied to `dst` as they are encoded (pages, dictionaries, statistics), in input order, without being decoded. The output has the first input's schema and key-value metadata, and the inputs' column orders, page indexes and bloom filters.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `sources` | `Sequence[str \| PathLike[str]]` | The Parquet files to concatenate, in order. Each must have the first one's Parquet schema, key-value metadata and column orders. | *required* |
| `dst` | `str \| PathLike[str]` | The file to write, created or truncated. It is removed if the copy fails part way, and must not be one of `sources`. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `int` | The number of rows written. |

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | If there are no sources, `dst` is one of them, a source is not a Parquet file, or the sources differ in schema, key-value metadata or column orders. |
| `OSError` | If a source cannot be opened or `dst` cannot be written. |
