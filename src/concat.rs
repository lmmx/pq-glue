//! Concatenation by copying encoded row groups.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parquet::basic::{ColumnOrder, SortOrder};
use parquet::bloom_filter::Sbbf;
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{WriterProperties, WriterVersion};
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

/// The value `get` gives every input, if they all give the same one.
fn shared<T: PartialEq>(inputs: &[Input], get: impl Fn(&Input) -> T) -> Option<T> {
    let first = get(&inputs[0]);
    inputs[1..].iter().all(|i| get(i) == first).then_some(first)
}

/// The inputs' `created_by`, if they all have the same one: it names the writer that
/// encoded every column chunk, which readers use to work around known writers' bugs.
fn created_by(inputs: &[Input]) -> Option<String> {
    shared(inputs, |i| {
        i.metadata.file_metadata().created_by().map(str::to_owned)
    })
    .flatten()
}

/// The inputs' format version, if they all have the same one.
fn writer_version(inputs: &[Input]) -> Option<WriterVersion> {
    match shared(inputs, |i| i.metadata.file_metadata().version())? {
        1 => Some(WriterVersion::PARQUET_1_0),
        2 => Some(WriterVersion::PARQUET_2_0),
        _ => None,
    }
}

/// The sorting columns every row group of every input declares, if they all declare the
/// same. A row group's sorting columns say how its own rows are sorted, and the writer
/// sets one value for every row group, so row groups that differ declare none.
fn sorting_columns(inputs: &[Input]) -> Option<Vec<SortingColumn>> {
    let mut row_groups = inputs.iter().flat_map(|i| i.metadata.row_groups());
    let first = row_groups.next()?.sorting_columns().cloned();
    row_groups
        .all(|rg| rg.sorting_columns() == first.as_ref())
        .then_some(first)
        .flatten()
}

/// Write the inputs' row groups to `output`, each column chunk appended as it is
/// encoded, with its bloom filter and page index entries where it has them. The footer
/// keeps the inputs' `created_by`, format version and row groups' sorting columns where
/// they all agree on them, parquet-rs's own (and no sorting columns) where they don't.
fn copy_row_groups(inputs: &[Input], output: &Path) -> Result<u64> {
    let fm = inputs[0].metadata.file_metadata();
    let mut props = WriterProperties::builder()
        .set_key_value_metadata(fm.key_value_metadata().cloned())
        .set_sorting_columns(sorting_columns(inputs));
    if let Some(created_by) = created_by(inputs) {
        props = props.set_created_by(created_by);
    }
    if let Some(version) = writer_version(inputs) {
        props = props.set_writer_version(version);
    }
    let props = props.build();
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
