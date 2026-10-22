//! The `pq_glue._pq_glue` Python extension module (feature `python`, built by maturin).

use std::path::PathBuf;

use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;

use crate::error::Error;

impl From<Error> for PyErr {
    fn from(e: Error) -> Self {
        match e {
            Error::Io { .. } => PyOSError::new_err(e.to_string()),
            e => PyValueError::new_err(e.to_string()),
        }
    }
}

/// Concatenate Parquet files with one schema into `dst`, copying their encoded row
/// groups without decoding them; returns the rows written. See `pq_glue/__init__.pyi`.
#[pyfunction]
#[pyo3(name = "concat")]
fn py_concat(py: Python<'_>, sources: Vec<PathBuf>, dst: PathBuf) -> PyResult<u64> {
    Ok(py.detach(|| crate::concat(&sources, &dst))?)
}

#[pymodule]
fn _pq_glue(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_concat, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
