//! Concatenate Parquet files that share one schema into one file, copying each row
//! group's encoded column chunks (pages, dictionaries, statistics) as they are: nothing
//! is decoded or re-encoded, so the cost is the bytes copied, whatever the nesting.
//!
//! The output has the inputs' row groups in order, the first input's schema and
//! key-value metadata (`ARROW:schema`, which Arrow readers need for the files' Arrow
//! types), the inputs' page indexes and bloom filters where they have them, and the
//! inputs' column orders, under which their statistics were computed (see
//! [`restore_column_orders`]).

use parquet::basic::{ColumnOrder, SortOrder};
use parquet::bloom_filter::Sbbf;
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn read_metadata(path: &Path) -> Result<(File, ParquetMetaData)> {
    let file = File::open(path)
        .map_err(|e| ParquetError::External(format!("{}: {e}", path.display()).into()))?;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&file)?;
    Ok((file, metadata))
}

/// Concatenate `inputs`, in order, into `output`, returning the rows written.
///
/// Every input must have the first one's Parquet schema and key-value metadata, or
/// nothing is written. `output` is created (or truncated) and is removed if the copy
/// fails part way; it must not be one of the inputs.
pub fn concat(inputs: &[PathBuf], output: &Path) -> Result<u64> {
    if inputs.is_empty() {
        return Err(ParquetError::General("no input files".into()));
    }
    if inputs.iter().any(|p| p.as_path() == output) {
        return Err(ParquetError::General(
            "the output is one of the inputs".into(),
        ));
    }
    let inputs = inputs
        .iter()
        .map(|p| read_metadata(p).map(|(f, m)| (p, f, m)))
        .collect::<Result<Vec<_>>>()?;
    let first = inputs[0].2.file_metadata();
    for (path, _, m) in &inputs[1..] {
        if m.file_metadata().schema() != first.schema() {
            return Err(ParquetError::General(format!(
                "{} has another schema than {}",
                path.display(),
                inputs[0].0.display()
            )));
        }
        if m.file_metadata().column_orders() != first.column_orders() {
            return Err(ParquetError::General(format!(
                "{} has other column orders than {}",
                path.display(),
                inputs[0].0.display()
            )));
        }
        if m.file_metadata().key_value_metadata() != first.key_value_metadata() {
            return Err(ParquetError::General(format!(
                "{} has other key-value metadata than {}",
                path.display(),
                inputs[0].0.display()
            )));
        }
    }
    let props = WriterProperties::builder()
        .set_key_value_metadata(first.key_value_metadata().cloned())
        .build();
    // Inputs written without column orders (before parquet-format 2.4) have statistics
    // in each type's legacy (signed) order, which is the type-defined order of floats
    let orders = first.column_orders().cloned().unwrap_or_else(|| {
        let n = first.schema_descr().num_columns();
        vec![ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED); n]
    });
    let result = write(
        &inputs,
        output,
        first.schema_descr().root_schema_ptr(),
        props,
    )
    .and_then(|rows| restore_column_orders(output, &orders).map(|()| rows));
    if result.is_err() {
        let _ = std::fs::remove_file(output);
    }
    result
}

fn write(
    inputs: &[(&PathBuf, File, ParquetMetaData)],
    output: &Path,
    schema: parquet::schema::types::TypePtr,
    props: WriterProperties,
) -> Result<u64> {
    let mut writer = SerializedFileWriter::new(File::create(output)?, schema, Arc::new(props))?;
    let mut rows = 0;
    for (_, file, metadata) in inputs {
        let page_index = metadata.page_index();
        for (r, rg) in metadata.row_groups().iter().enumerate() {
            let mut rg_out = writer.next_row_group()?;
            for (c, column) in rg.columns().iter().enumerate() {
                rg_out.append_column(
                    file,
                    ColumnCloseResult {
                        bytes_written: column.compressed_size() as _,
                        rows_written: rg.num_rows() as _,
                        metadata: column.clone(),
                        bloom_filter: Sbbf::read_from_column_chunk(column, file).ok().flatten(),
                        column_index: page_index.and_then(|pi| pi.column_index(r, c)).cloned(),
                        offset_index: page_index.and_then(|pi| pi.offset_index(r, c)).cloned(),
                    },
                )?;
            }
            rg_out.close()?;
            rows += rg.num_rows() as u64;
        }
    }
    writer.close()?;
    Ok(rows)
}

/// A `ColumnOrder` union in Thrift's compact encoding: a field header (field 1,
/// TYPE_ORDER, or field 2, IEEE_754_TOTAL_ORDER, each an empty struct), the empty
/// struct's stop byte, and the union's.
const TYPE_DEFINED: [u8; 3] = [0x1C, 0x00, 0x00];
const IEEE_754: [u8; 3] = [0x2C, 0x00, 0x00];

/// Put the inputs' column orders back in the footer parquet-rs wrote to `path`.
///
/// parquet-rs writes every float column's order as IEEE 754 total order, whatever the
/// order its statistics were computed in; the copied statistics were computed in the
/// inputs' (for pyarrow's, type-defined) order, and Arrow C++ ignores IEEE 754 float
/// min/max without the NaN counts that order requires. The writer's API has no way to
/// set the orders, and the two encodings are the same length, so the footer is patched
/// in place: its `column_orders` list is its last field, each element checked to be
/// one of the two before it is rewritten, and the footer is read back to check it has
/// exactly `orders`. Nothing else in the file changes.
pub fn restore_column_orders(path: &Path, orders: &[ColumnOrder]) -> Result<()> {
    let mut want = Vec::with_capacity(orders.len());
    for order in orders {
        want.push(match order {
            ColumnOrder::TYPE_DEFINED_ORDER(_) => TYPE_DEFINED,
            ColumnOrder::IEEE_754_TOTAL_ORDER => IEEE_754,
            other => {
                return Err(ParquetError::General(format!(
                    "cannot write column order {other:?}"
                )))
            }
        });
    }
    let mut f = OpenOptions::new().read(true).write(true).open(path)?;
    let len = f.seek(SeekFrom::End(0))?;
    let mut tail = [0u8; 8];
    f.seek(SeekFrom::End(-8))?;
    f.read_exact(&mut tail)?;
    if &tail[4..] != b"PAR1" {
        return Err(ParquetError::General("output has no Parquet footer".into()));
    }
    let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;
    let footer_start = len - 8 - footer_len;
    let mut footer = vec![0u8; footer_len as usize];
    f.seek(SeekFrom::Start(footer_start))?;
    f.read_exact(&mut footer)?;
    // [list header][n x 3 bytes][FileMetaData's stop byte]
    let n = want.len();
    let unexpected =
        || ParquetError::General("unexpected column_orders encoding in the footer".into());
    let elems_end = footer.len().checked_sub(1).ok_or_else(unexpected)?;
    let elems_start = elems_end.checked_sub(3 * n).ok_or_else(unexpected)?;
    if footer[elems_end] != 0x00 {
        return Err(unexpected());
    }
    let header = list_header(n);
    if elems_start < header.len() || footer[elems_start - header.len()..elems_start] != header[..] {
        return Err(unexpected());
    }
    for (i, w) in want.iter().enumerate() {
        let e = &mut footer[elems_start + 3 * i..elems_start + 3 * i + 3];
        if *e != TYPE_DEFINED && *e != IEEE_754 {
            return Err(unexpected());
        }
        e.copy_from_slice(w);
    }
    f.seek(SeekFrom::Start(footer_start + elems_start as u64))?;
    f.write_all(&footer[elems_start..elems_end])?;
    drop(f);
    let got = read_metadata(path)?.1;
    if got.file_metadata().column_orders().map(Vec::as_slice) != Some(orders) {
        return Err(ParquetError::General("column orders not restored".into()));
    }
    Ok(())
}

/// A compact-protocol list header for `n` structs: the size in the high nibble below
/// 15, else 0xF and the size as a varint.
fn list_header(n: usize) -> Vec<u8> {
    if n < 15 {
        return vec![((n as u8) << 4) | 0x0C];
    }
    let mut out = vec![0xFC];
    let mut v = n as u64;
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return out;
        }
        out.push(b | 0x80);
    }
}

#[cfg(feature = "python")]
mod python {
    use super::*;
    use pyo3::exceptions::{PyOSError, PyValueError};
    use pyo3::prelude::*;

    fn to_py(e: ParquetError) -> PyErr {
        match e {
            ParquetError::External(e) => PyOSError::new_err(e.to_string()),
            e => PyValueError::new_err(e.to_string()),
        }
    }

    /// Concatenate Parquet files with one schema into `dst`, copying their encoded row
    /// groups without decoding them; returns the rows written. Raises ValueError if the
    /// files differ in schema or key-value metadata, OSError if one cannot be read.
    #[pyfunction]
    #[pyo3(name = "concat")]
    fn py_concat(py: Python<'_>, sources: Vec<PathBuf>, dst: PathBuf) -> PyResult<u64> {
        py.detach(|| super::concat(&sources, &dst)).map_err(to_py)
    }

    #[pymodule]
    fn _pq_glue(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_function(wrap_pyfunction!(py_concat, m)?)?;
        m.add("__version__", env!("CARGO_PKG_VERSION"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::list_header;

    #[test]
    fn list_headers() {
        assert_eq!(list_header(0), vec![0x0C]);
        assert_eq!(list_header(14), vec![0xEC]);
        assert_eq!(list_header(15), vec![0xFC, 15]);
        assert_eq!(list_header(93), vec![0xFC, 93]);
        assert_eq!(list_header(300), vec![0xFC, 0xAC, 0x02]);
    }
}
