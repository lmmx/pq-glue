//! The `pq_glue._pq_glue` Python extension module (feature `python`, built by maturin).

use std::path::{Path, PathBuf};

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::SchemaRef;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::CdcOptions;
use pyo3::exceptions::{PyOSError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};

use crate::error::Error;
use crate::write::{write_batches, WriteOptions};

impl From<Error> for PyErr {
    fn from(e: Error) -> Self {
        match e {
            Error::Io { .. } => PyOSError::new_err(e.to_string()),
            e => PyValueError::new_err(e.to_string()),
        }
    }
}

/// A source as Python gives it: a file (all its row groups), or a file and the row
/// groups to copy from it, in order.
#[derive(FromPyObject)]
enum PySource {
    Whole(PathBuf),
    RowGroups(PathBuf, Vec<usize>),
}

impl From<PySource> for crate::Source {
    fn from(s: PySource) -> Self {
        match s {
            PySource::Whole(path) => crate::Source::whole(path),
            PySource::RowGroups(path, rgs) => crate::Source::row_groups(path, rgs),
        }
    }
}

fn sources_of(sources: Vec<PySource>) -> Vec<crate::Source> {
    sources.into_iter().map(Into::into).collect()
}

/// Concatenate Parquet files with one schema (or chosen row groups of them) into `dst`,
/// copying their encoded row groups without decoding them; returns the rows written.
/// See `pq_glue/_pq_glue.pyi`.
#[pyfunction]
#[pyo3(name = "concat")]
fn py_concat(py: Python<'_>, sources: Vec<PySource>, dst: PathBuf) -> PyResult<u64> {
    let sources = sources_of(sources);
    Ok(py.detach(|| crate::concat_sources(&sources, &dst))?)
}

/// Concatenate each job's sources into its destination, the jobs in parallel; returns
/// each job's rows written. See `pq_glue/_pq_glue.pyi`.
#[pyfunction]
#[pyo3(name = "concat_many")]
fn py_concat_many(py: Python<'_>, jobs: Vec<(Vec<PySource>, PathBuf)>) -> PyResult<Vec<u64>> {
    let jobs: Vec<(Vec<crate::Source>, PathBuf)> = jobs
        .into_iter()
        .map(|(sources, dst)| (sources_of(sources), dst))
        .collect();
    let results = py.detach(|| crate::concat_many_sources(&jobs));
    jobs.iter()
        .zip(results)
        .map(|((_, dst), result)| result.map_err(|e| job_error(dst, e)))
        .collect()
}

/// A failed job's error, as `concat` raises it, naming the job's destination.
fn job_error(dst: &Path, e: Error) -> PyErr {
    let message = format!("{} not written: {e}", dst.display());
    match e {
        Error::Io { .. } => PyOSError::new_err(message),
        _ => PyValueError::new_err(message),
    }
}

/// The stream of `data`, any object of the Arrow PyCapsule interface
/// (`__arrow_c_stream__`: a pyarrow table or record batch reader, a Polars frame, ...).
fn stream_of(data: &Bound<'_, PyAny>) -> PyResult<ArrowArrayStreamReader> {
    if !data.hasattr("__arrow_c_stream__")? {
        return Err(PyTypeError::new_err(format!(
            "Expected Arrow data with an __arrow_c_stream__ method, not {}",
            data.get_type().name()?
        )));
    }
    let capsule = data.call_method0("__arrow_c_stream__")?;
    let capsule = capsule.cast::<PyCapsule>().map_err(|_| {
        PyTypeError::new_err(
            "__arrow_c_stream__ must return a PyCapsule named 'arrow_array_stream'",
        )
    })?;
    let pointer = capsule.pointer_checked(Some(c"arrow_array_stream"))?;
    // SAFETY: a capsule named `arrow_array_stream` holds an FFI_ArrowArrayStream (the Arrow
    // PyCapsule interface); `from_raw` moves it out, leaving the capsule's released, as a
    // consumer of the interface does
    unsafe { ArrowArrayStreamReader::from_raw(pointer.cast::<FFI_ArrowArrayStream>().as_ptr()) }
        .map_err(|e| PyValueError::new_err(format!("Invalid Arrow stream: {e}")))
}

/// The stream's record batches: their buffers are taken over, not copied.
fn batches_of(reader: ArrowArrayStreamReader) -> PyResult<Vec<RecordBatch>> {
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| PyValueError::new_err(format!("Failed to read the Arrow stream: {e}")))
}

fn compression(name: &str, level: Option<i32>) -> PyResult<Compression> {
    let invalid = |e: parquet::errors::ParquetError| PyValueError::new_err(e.to_string());
    match (name, level) {
        ("zstd", level) => Ok(Compression::ZSTD(
            ZstdLevel::try_new(level.unwrap_or(1)).map_err(invalid)?,
        )),
        ("snappy", None) => Ok(Compression::SNAPPY),
        ("none" | "uncompressed", None) => Ok(Compression::UNCOMPRESSED),
        ("snappy" | "none" | "uncompressed", Some(_)) => Err(PyValueError::new_err(format!(
            "compression_level is for zstd, not {name}"
        ))),
        _ => Err(PyValueError::new_err(format!(
            "compression must be 'zstd', 'snappy' or 'none', not {name:?}"
        ))),
    }
}

/// Each sorting column: a leaf column's path (`"id"`, `"a.b"`) or index, optionally with
/// `descending` and `nulls_first`, as a tuple.
fn sorting_columns(
    columns: Vec<Bound<'_, PyAny>>,
    schema: &SchemaRef,
    spec_names: bool,
) -> PyResult<Vec<SortingColumn>> {
    let descr = parquet::arrow::ArrowSchemaConverter::new()
        .with_coerce_types(spec_names)
        .convert(schema)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let leaf = |column: &Bound<'_, PyAny>| -> PyResult<i32> {
        if let Ok(i) = column.extract::<usize>() {
            if i < descr.num_columns() {
                return Ok(i as i32);
            }
            return Err(PyValueError::new_err(format!("no leaf column {i}")));
        }
        let path: String = column.extract()?;
        (0..descr.num_columns())
            .find(|&i| descr.column(i).path().string() == path)
            .map(|i| i as i32)
            .ok_or_else(|| PyValueError::new_err(format!("no leaf column {path:?}")))
    };
    columns
        .iter()
        .map(|c| {
            let (column, descending, nulls_first) =
                match c.extract::<(Bound<'_, PyAny>, bool, bool)>() {
                    Ok(t) => t,
                    Err(_) => (c.clone(), false, false),
                };
            Ok(SortingColumn {
                column_idx: leaf(&column)?,
                descending,
                nulls_first,
            })
        })
        .collect()
}

fn chunking(value: &Bound<'_, PyAny>) -> PyResult<Option<CdcOptions>> {
    if let Ok(enabled) = value.extract::<bool>() {
        return Ok(enabled.then(CdcOptions::default));
    }
    let dict = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("content_defined_chunking must be a bool or a dict"))?;
    let mut options = CdcOptions::default();
    for (key, v) in dict.iter() {
        match key.extract::<String>()?.as_str() {
            "min_chunk_size" => options.min_chunk_size = v.extract()?,
            "max_chunk_size" => options.max_chunk_size = v.extract()?,
            "norm_level" => options.norm_level = v.extract()?,
            other => {
                return Err(PyValueError::new_err(format!(
                    "content_defined_chunking has no option {other:?}"
                )))
            }
        }
    }
    Ok(Some(options))
}

/// Write Arrow data to a Parquet file with pq-glue's writer; returns the rows written.
/// See `pq_glue/_pq_glue.pyi`.
#[pyfunction]
#[pyo3(name = "write_parquet")]
#[pyo3(signature = (
    data, path, *, row_group_size=None, compression="zstd", compression_level=None,
    spec_names=true, adaptive_dictionary=true, sorting_columns=None,
    content_defined_chunking=None, max_workers=None
))]
#[allow(clippy::too_many_arguments)]
fn py_write_parquet(
    py: Python<'_>,
    data: Bound<'_, PyAny>,
    path: PathBuf,
    row_group_size: Option<usize>,
    compression: &str,
    compression_level: Option<i32>,
    spec_names: bool,
    adaptive_dictionary: bool,
    sorting_columns: Option<Vec<Bound<'_, PyAny>>>,
    content_defined_chunking: Option<Bound<'_, PyAny>>,
    max_workers: Option<usize>,
) -> PyResult<u64> {
    if max_workers == Some(0) {
        return Err(PyValueError::new_err("max_workers must be at least 1"));
    }
    if row_group_size == Some(0) {
        return Err(PyValueError::new_err("row_group_size must be at least 1"));
    }
    let reader = stream_of(&data)?;
    let schema = reader.schema();
    let options = WriteOptions {
        max_row_group_rows: row_group_size,
        spec_names,
        compression: self::compression(compression, compression_level)?,
        adaptive_dictionary,
        sorting_columns: sorting_columns
            .map(|c| self::sorting_columns(c, &schema, spec_names))
            .transpose()?,
        content_defined_chunking: match content_defined_chunking {
            Some(v) => chunking(&v)?,
            None => None,
        },
        max_workers,
    };
    // The stream read, and the batches encoded, without the GIL: a producer that needs
    // it (a Python record batch reader) takes it for itself
    py.detach(|| {
        let batches = batches_of(reader)?;
        Ok(write_batches(&path, schema, batches, &options)?)
    })
}

/// Read a Parquet file's row groups (all if `row_groups` is None), each decoded in
/// parallel, as an Arrow C stream capsule. See `pq_glue/_pq_glue.pyi`.
#[pyfunction]
#[pyo3(name = "read_parquet")]
#[pyo3(signature = (path, row_groups=None))]
fn py_read_parquet<'py>(
    py: Python<'py>,
    path: PathBuf,
    row_groups: Option<Vec<usize>>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let (schema, batches) = py.detach(|| crate::read::read(&path, row_groups.as_deref()))?;
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    // The capsule owns the stream: a consumer moves it out (releasing the capsule's), and
    // one never consumed is released when the capsule is freed
    PyCapsule::new_with_value(
        py,
        FFI_ArrowArrayStream::new(Box::new(reader)),
        c"arrow_array_stream",
    )
}

#[pymodule]
fn _pq_glue(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_concat, m)?)?;
    m.add_function(wrap_pyfunction!(py_concat_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_parquet, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
