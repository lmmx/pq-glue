//! Opening the input files and checking they can be concatenated.

use std::fs::File;
use std::path::{Path, PathBuf};

use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

use crate::error::{Error, Result};

/// An input file, open, with its footer (and page indexes, if it has them) read.
pub(crate) struct Input {
    pub path: PathBuf,
    pub file: File,
    pub metadata: ParquetMetaData,
}

impl Input {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(Error::io(path))?;
        let metadata = read_metadata(path, &file)?;
        Ok(Input {
            path: path.to_path_buf(),
            file,
            metadata,
        })
    }
}

/// A Parquet file's footer, and its page indexes where it has them.
pub(crate) fn read_metadata(path: &Path, file: &File) -> Result<ParquetMetaData> {
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(file)
        .map_err(Error::parquet(path))
}

/// Every input has the first one's Parquet schema, column orders and key-value
/// metadata: the output's footer is the first input's, so its row groups must all be
/// read and compared the same way.
pub(crate) fn check_compatible(inputs: &[Input]) -> Result<()> {
    let first = &inputs[0];
    let fm = first.metadata.file_metadata();
    for input in &inputs[1..] {
        let m = input.metadata.file_metadata();
        let what = if m.schema() != fm.schema() {
            "schema"
        } else if m.column_orders() != fm.column_orders() {
            "column orders"
        } else if m.key_value_metadata() != fm.key_value_metadata() {
            "key-value metadata"
        } else {
            continue;
        };
        return Err(Error::Incompatible {
            path: input.path.clone(),
            first: first.path.clone(),
            what,
        });
    }
    Ok(())
}
