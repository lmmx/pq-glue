//! Opening the inputs and checking they can be concatenated.

use std::fs::File;
use std::path::{Path, PathBuf};

use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::ChunkReader;

use crate::error::{Error, Result};

/// An input, open, with its footer (and page indexes, if it has them) read: a file, or
/// (for [`write`](crate::write)) a row group encoded in memory. `path` names it in errors.
pub(crate) struct Input<R = File> {
    pub path: PathBuf,
    pub file: R,
    pub metadata: ParquetMetaData,
}

impl<R: ChunkReader> Input<R> {
    pub fn new(path: PathBuf, file: R) -> Result<Self> {
        let metadata = read_metadata(&path, &file)?;
        Ok(Input {
            path,
            file,
            metadata,
        })
    }
}

/// A Parquet file's footer, and its page indexes where it has them.
pub(crate) fn read_metadata<R: ChunkReader>(path: &Path, file: &R) -> Result<ParquetMetaData> {
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(file)
        .map_err(Error::parquet(path))
}

/// `input` has `first`'s Parquet schema, column orders and key-value metadata.
pub(crate) fn check_same<R, S>(first: &Input<R>, input: &Input<S>) -> Result<()> {
    let fm = first.metadata.file_metadata();
    let m = input.metadata.file_metadata();
    let what = if m.schema() != fm.schema() {
        "schema"
    } else if m.column_orders() != fm.column_orders() {
        "column orders"
    } else if m.key_value_metadata() != fm.key_value_metadata() {
        "key-value metadata"
    } else {
        return Ok(());
    };
    Err(Error::Incompatible {
        path: input.path.clone(),
        first: first.path.clone(),
        what,
    })
}
