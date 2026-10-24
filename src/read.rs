//! Parquet read to Arrow record batches, its row groups decoded in parallel (feature
//! `read`).
//!
//! Each row group is decoded by a reader of its own on rayon, into one record batch. The
//! whole file is read into memory once when every row group is asked for; for some of
//! them, each reader opens the file itself and reads only its row group's column chunks
//! (readers of one `File` would share its offset). The batches are returned in the order
//! asked for, with the file's Arrow schema (from its `ARROW:schema` metadata, as the
//! writer stored it).

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use rayon::prelude::*;

use crate::error::{Error, Result};

/// The Arrow schema of the file at `path` and its rows: one record batch for each of
/// `row_groups` (all of them if `None`), in the order given, each row group decoded in
/// parallel.
pub fn read(path: &Path, row_groups: Option<&[usize]>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    // The whole file in memory for a whole read; only the footer for some row groups
    let whole = match row_groups {
        None => Some(Bytes::from(std::fs::read(path).map_err(Error::io(path))?)),
        Some(_) => None,
    };
    let meta = match &whole {
        Some(bytes) => ArrowReaderMetadata::load(bytes, ArrowReaderOptions::new()),
        None => ArrowReaderMetadata::load(
            &File::open(path).map_err(Error::io(path))?,
            ArrowReaderOptions::new(),
        ),
    }
    .map_err(Error::parquet(path))?;
    let n = meta.metadata().num_row_groups();
    let groups: Vec<usize> = match row_groups {
        Some(groups) => groups.to_vec(),
        None => (0..n).collect(),
    };
    if let Some(&g) = groups.iter().find(|&&g| g >= n) {
        return Err(Error::RowGroup {
            path: path.into(),
            row_group: g,
            row_groups: n,
        });
    }
    let schema = Arc::clone(meta.schema());
    let batches = groups
        .par_iter()
        .map(|&g| match &whole {
            Some(bytes) => read_row_group(path, bytes.clone(), &meta, g),
            None => read_row_group(path, File::open(path).map_err(Error::io(path))?, &meta, g),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((schema, batches))
}

fn read_row_group<R: parquet::file::reader::ChunkReader + 'static>(
    path: &Path,
    file: R,
    meta: &ArrowReaderMetadata,
    g: usize,
) -> Result<RecordBatch> {
    let rows = meta.metadata().row_group(g).num_rows() as usize;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta.clone())
        .with_row_groups(vec![g])
        .with_batch_size(rows.max(1))
        .build()
        .map_err(Error::parquet(path))?;
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| arrow_error(path, e))?;
    match batches.len() {
        0 => Ok(RecordBatch::new_empty(Arc::clone(meta.schema()))),
        1 => Ok(batches.into_iter().next().unwrap()),
        _ => arrow_select_concat(meta.schema(), &batches, path),
    }
}

fn arrow_select_concat(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    path: &Path,
) -> Result<RecordBatch> {
    arrow_select::concat::concat_batches(schema, batches).map_err(|e| arrow_error(path, e))
}

fn arrow_error(path: &Path, e: arrow_schema::ArrowError) -> Error {
    Error::Parquet {
        path: path.into(),
        source: parquet::errors::ParquetError::ArrowError(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int64Type;
    use arrow_array::{ArrayRef, Int64Array, ListArray, StringArray, StructArray};
    use arrow_schema::{DataType, Field};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;

    fn batch(start: i64, n: i64) -> RecordBatch {
        let ids: ArrayRef = Arc::new(StringArray::from_iter_values(
            (start..start + n).map(|i| format!("Q{i}")),
        ));
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            (start..start + n).map(|i| Some((0..i % 3).map(Some).collect::<Vec<_>>())),
        ));
        let inner: ArrayRef = Arc::new(Int64Array::from_iter_values(start..start + n));
        let st: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("x", DataType::Int64, false)),
            inner,
        )]));
        RecordBatch::try_from_iter(vec![("id", ids), ("l", lists), ("s", st)]).unwrap()
    }

    #[test]
    fn row_groups_read_in_parallel_are_the_file_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.parquet");
        let first = batch(0, 100);
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(30))
            .build();
        let mut w = ArrowWriter::try_new(File::create(&path).unwrap(), first.schema(), Some(props))
            .unwrap();
        w.write(&first).unwrap();
        w.close().unwrap();
        let (schema, batches) = read(&path, None).unwrap();
        assert_eq!(schema, first.schema());
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            [30, 30, 30, 10]
        );
        assert_eq!(
            arrow_select::concat::concat_batches(&schema, &batches).unwrap(),
            first
        );
        let (_, some) = read(&path, Some(&[3, 1])).unwrap();
        assert_eq!(some[0], first.slice(90, 10));
        assert_eq!(some[1], first.slice(30, 30));
        assert!(matches!(
            read(&path, Some(&[4])),
            Err(Error::RowGroup { row_group: 4, .. })
        ));
    }
}
