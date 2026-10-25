# pq\_glue

Glue Parquet files together, and write Arrow data to Parquet and read it back.

`concat` copies encoded row groups without decoding them (`concat_many` for many concatenations at once, `concat_size` for the size of its file without writing it); `write_parquet` writes Arrow data with a choice of engines; `read_parquet` reads a file's row groups in parallel.

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

## concat\_many

```python
concat_many(
    jobs: Sequence[tuple[Sequence[str | PathLike[str]], str | PathLike[str]]],
) -> list[int]
```

Concatenate many sets of Parquet files at once, each into its own file.

Each job, `(sources, dst)`, is a `concat(sources, dst)`, with the same output; the jobs run in parallel, on a thread per CPU, with the GIL released for the whole batch. For many small concatenations (each a few files), whose time goes on opening files and writing footers more than on bytes copied, one call is quicker than a `concat` per job.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `jobs` | `Sequence[tuple[Sequence[str \| PathLike[str]], str \| PathLike[str]]]` | `(sources, dst)` tuples, as `concat` takes them. No two jobs may have the same `dst`, nor one job's `dst` be another's source. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `list[int]` | The number of rows written for each job, in the jobs' order. |

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | For the first job (in the jobs' order) that fails as `concat` raises ValueError, naming its `dst`. |
| `OSError` | For the first job that fails as `concat` raises OSError, naming its `dst`. |

> [!NOTE]
>
> **Notes**
>
> Every job runs, whether or not another fails: a job that fails leaves no `dst`, as `concat` leaves none, and the others' are written.

## concat\_size

```python
concat_size(sources: Sequence[str | PathLike[str]]) -> int
```

The size of the file `concat(sources, dst)` would write, without writing it.

The sources are opened and checked as `concat` checks them, and their footers, page indexes and bloom filters read; their column chunks are not read. The file is glued as `concat` glues it, its bytes counted and not kept, so the size is exactly that of `concat`'s file.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `sources` | `Sequence[str \| PathLike[str]]` | The Parquet files `concat` would concatenate, in order. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `int` | The size in bytes of the file `concat` would write. |

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | As `concat` raises it: no sources, a source not a Parquet file, or sources that differ in schema, key-value metadata or column orders. |
| `OSError` | If a source cannot be opened. |

## read\_parquet

```python
read_parquet(path: str | PathLike[str], row_groups: Sequence[int] | None = None)
```

Read a Parquet file to a `pyarrow.Table`, each row group decoded in parallel.

The table has the file's Arrow schema (from its `ARROW:schema` metadata) and one chunk per row group read, in the order of `row_groups` (all of them, in order, if None). The values are those `pyarrow.parquet.ParquetFile.read` and `read_row_groups` give; the file is read into memory once, then decoded by arrow-rs on every core, without the GIL.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | If the file is not Parquet, or a row group asked for is not in it. |
| `OSError` | If the file cannot be read. |

## write\_parquet

```python
write_parquet(
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
) -> int
```

Write Arrow data to a Parquet file with the engine asked for; returns the rows written.

Every engine writes the same values, in the same order, with the same Arrow schema, row groups, compression, sorting columns and content-defined chunking. Their bytes differ (page boundaries, dictionaries, the footer's `created_by` and format version): see the documentation's engines page. An option an engine lacks raises `ValueError`.

Parameters:

<table>       <thead>         <tr>           <th>Name</th>           <th>Type</th>           <th>Description</th>           <th>Default</th>         </tr>       </thead>       <tbody>           <tr class="doc-section-item">             <td>                 <code>data</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="object" optional="">object</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <p>Any object with an <code>__arrow_c_stream__</code> method: a pyarrow table or record batch reader, a Polars data frame, ...</p>               </div>             </td>             <td>                 <em>required</em>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>path</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="str" optional="">str</autoref> | <autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" hover="" identifier="os.PathLike" optional="">PathLike</autoref>[<autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="str" optional="">str</autoref>]</code>             </td>             <td>               <div class="doc-md-description">                 <p>The file to write, created or truncated.</p>               </div>             </td>             <td>                 <em>required</em>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>engine</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" hover="" identifier="pq_glue._write.Engine" optional="">Engine</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <ul> <li>&quot;native&quot;: arrow-rs, polars-genson's writer, on one thread.</li> <li>&quot;parallel&quot;: the same writer, on <code>max_workers</code> threads (by default one per   CPU): each row group's leaf columns in parallel, or, with content-defined   chunking, whole row groups in parallel, glued in order. The same bytes as   &quot;native&quot;, sooner, holding a row group in flight per thread.</li> <li>&quot;pyarrow&quot;: pyarrow's writer (pyarrow needed: <code>pq-glue[pyarrow]</code>), on one   thread, or with <code>max_workers</code> above 1 a row group per thread, glued in order.</li> </ul>               </div>             </td>             <td>                   <code>'parallel'</code>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>max_workers</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="int" optional="">int</autoref> | None</code>             </td>             <td>               <div class="doc-md-description">                 <p>Threads that encode at once; by default 1 for &quot;native&quot; and &quot;pyarrow&quot;, one per CPU for &quot;parallel&quot;.</p>               </div>             </td>             <td>                   <code>None</code>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>row_group_size</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="int" optional="">int</autoref> | None</code>             </td>             <td>               <div class="doc-md-description">                 <p>Rows per row group; 1,048,576 if None.</p>               </div>             </td>             <td>                   <code>None</code>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>compression</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="str" optional="">str</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <p>&quot;zstd&quot; (the default), &quot;snappy&quot; or &quot;none&quot;.</p>               </div>             </td>             <td>                   <code>'zstd'</code>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>compression_level</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="int" optional="">int</autoref> | None</code>             </td>             <td>               <div class="doc-md-description">                 <p>The zstd level; 1 if None.</p>               </div>             </td>             <td>                   <code>None</code>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>sorting_columns</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" hover="" identifier="collections.abc.Sequence" optional="">Sequence</autoref>[<autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" hover="" identifier="pq_glue._write.SortingColumn" optional="">SortingColumn</autoref>] | None</code>             </td>             <td>               <div class="doc-md-description">                 <p>The sorting columns each row group declares: each a leaf column's path (<code>&quot;id&quot;</code>, <code>&quot;a.b&quot;</code>) or index, or a tuple of that, <code>descending</code> and <code>nulls_first</code>. The rows must already be in that order.</p>               </div>             </td>             <td>                   <code>None</code>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>content_defined_chunking</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="bool" optional="">bool</autoref> | <autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" hover="" identifier="collections.abc.Mapping" optional="">Mapping</autoref>[<autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="str" optional="">str</autoref>, <autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="int" optional="">int</autoref>] | None</code>             </td>             <td>               <div class="doc-md-description">                 <p>Content-defined chunking of data pages, for deduplicating storage: True for the defaults (256 KiB minimum, 1 MiB maximum chunk, normalisation level 0), or a dict of <code>min_chunk_size</code>, <code>max_chunk_size</code>, <code>norm_level</code>. Chunking starts afresh at each row group in every engine.</p>               </div>             </td>             <td>                   <code>None</code>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>spec_names</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="bool" optional="">bool</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <p>List and map groups take the Parquet specification's names (<code>list</code>, <code>element</code>, <code>key_value</code>).</p>               </div>             </td>             <td>                   <code>True</code>             </td>           </tr>           <tr class="doc-section-item">             <td>                 <code>adaptive_dictionary</code>             </td>             <td>                   <code><autoref backlink-anchor="pq_glue.write_parquet" backlink-type="used-by" identifier="bool" optional="">bool</autoref> | None</code>             </td>             <td>               <div class="doc-md-description">                 <p>(&quot;native&quot;, &quot;parallel&quot;) Leaf columns whose sampled values are more than three quarters distinct are not dictionary encoded; True if None. &quot;pyarrow&quot; encodes every leaf with a dictionary, and rejects True.</p>               </div>             </td>             <td>                   <code>None</code>             </td>           </tr>       </tbody>     </table>

Returns:

| Type | Description |
| --- | --- |
| `int` | The number of rows written. |
