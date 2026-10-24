# Concepts

A Parquet file is a sequence of row groups followed by a footer. Each row group has one column chunk per leaf column. The footer records the schema and, for every column chunk, where it starts, how long it is, its encodings, its compression and its statistics.

To concatenate files, pq-glue:

1. reads each input's footer (and page indexes, if any);
2. checks that the inputs share one schema, key-value metadata and column orders;
3. copies each input's column chunks, in order, to the output as bytes;
4. writes a new footer, with each column chunk's offsets moved to its new position;
5. restores the inputs' column orders in that footer ([Column orders](<https://lmmx.github.io/pq-glue/pr-preview/pr-7/concepts/column-orders/index.md>)).

No page is decompressed or decoded, so the cost is that of copying the bytes, and the memory used does not grow with the size of the files.

- [What is copied](<https://lmmx.github.io/pq-glue/pr-preview/pr-7/concepts/copying/index.md>)
- [Column orders](<https://lmmx.github.io/pq-glue/pr-preview/pr-7/concepts/column-orders/index.md>)
