//! Concatenation by copying encoded row groups.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parquet::basic::{ColumnOrder, SortOrder};
use parquet::bloom_filter::Sbbf;
use parquet::column::writer::ColumnCloseResult;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;

use crate::error::{Error, Result};
use crate::footer::restore_column_orders;
use crate::input::{check_compatible, Input};

/// Concatenate `inputs`, in order, into `output`, returning the rows written.
///
/// Every input must have the first one's Parquet schema, column orders and key-value
/// metadata, or nothing is written. `output` is created (or truncated) and is removed
/// if the copy fails part way; it must not be one of the inputs.
pub fn concat(inputs: &[PathBuf], output: &Path) -> Result<u64> {
    if inputs.is_empty() {
        return Err(Error::NoInputs);
    }
    if inputs.iter().any(|p| p.as_path() == output) {
        return Err(Error::OutputIsInput(output.to_path_buf()));
    }
    let inputs = inputs
        .iter()
        .map(|p| Input::open(p))
        .collect::<Result<Vec<_>>>()?;
    check_compatible(&inputs)?;
    let result = copy_row_groups(&inputs, output).and_then(|rows| {
        restore_column_orders(output, &column_orders(&inputs[0]))?;
        Ok(rows)
    });
    if result.is_err() {
        let _ = std::fs::remove_file(output);
    }
    result
}

/// The inputs' column orders. Files written without them (before parquet-format 2.4)
/// have statistics in each type's legacy (signed) order, which is the type-defined
/// order of floats.
fn column_orders(first: &Input) -> Vec<ColumnOrder> {
    let fm = first.metadata.file_metadata();
    fm.column_orders().cloned().unwrap_or_else(|| {
        let n = fm.schema_descr().num_columns();
        vec![ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED); n]
    })
}

/// Write the inputs' row groups to `output`, each column chunk appended as it is
/// encoded, with its bloom filter and page index entries where it has them.
fn copy_row_groups(inputs: &[Input], output: &Path) -> Result<u64> {
    let fm = inputs[0].metadata.file_metadata();
    let props = WriterProperties::builder()
        .set_key_value_metadata(fm.key_value_metadata().cloned())
        .build();
    let file = std::fs::File::create(output).map_err(Error::io(output))?;
    let mut writer =
        SerializedFileWriter::new(file, fm.schema_descr().root_schema_ptr(), Arc::new(props))
            .map_err(Error::parquet(output))?;
    let mut rows = 0;
    for input in inputs {
        let page_index = input.metadata.page_index();
        for (r, rg) in input.metadata.row_groups().iter().enumerate() {
            let mut rg_out = writer.next_row_group().map_err(Error::parquet(output))?;
            for (c, column) in rg.columns().iter().enumerate() {
                let chunk = ColumnCloseResult {
                    bytes_written: column.compressed_size() as _,
                    rows_written: rg.num_rows() as _,
                    metadata: column.clone(),
                    bloom_filter: Sbbf::read_from_column_chunk(column, &input.file)
                        .ok()
                        .flatten(),
                    column_index: page_index.and_then(|pi| pi.column_index(r, c)).cloned(),
                    offset_index: page_index.and_then(|pi| pi.offset_index(r, c)).cloned(),
                };
                rg_out
                    .append_column(&input.file, chunk)
                    .map_err(Error::parquet(&input.path))?;
            }
            rg_out.close().map_err(Error::parquet(output))?;
            rows += rg.num_rows() as u64;
        }
    }
    writer.close().map_err(Error::parquet(output))?;
    Ok(rows)
}
