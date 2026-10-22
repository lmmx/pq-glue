//! The errors [`concat`](crate::concat) returns.

use std::fmt;
use std::io;
use std::path::PathBuf;

use parquet::errors::ParquetError;

/// What [`concat`](crate::concat) and the footer checks can fail with.
#[derive(Debug)]
pub enum Error {
    /// No input files were given.
    NoInputs,
    /// The output path is one of the inputs.
    OutputIsInput(PathBuf),
    /// An input's Parquet schema, key-value metadata or column orders differ from the
    /// first input's.
    Incompatible {
        /// The input that differs.
        path: PathBuf,
        /// The first input, which the others must match.
        first: PathBuf,
        /// What differs: "schema", "key-value metadata" or "column orders".
        what: &'static str,
    },
    /// A file could not be opened, read or written.
    Io {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// A file's Parquet metadata could not be read, or the output could not be written.
    Parquet {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: ParquetError,
    },
    /// The footer parquet-rs wrote was not laid out as expected, so its column orders
    /// were not rewritten (see [`footer::restore_column_orders`](crate::footer::restore_column_orders)).
    Footer {
        /// The output file.
        path: PathBuf,
        /// What was unexpected.
        reason: String,
    },
}

/// A `Result` with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn io(path: impl Into<PathBuf>) -> impl FnOnce(io::Error) -> Self {
        let path = path.into();
        move |source| Error::Io { path, source }
    }

    pub(crate) fn parquet(path: impl Into<PathBuf>) -> impl FnOnce(ParquetError) -> Self {
        let path = path.into();
        move |source| Error::Parquet { path, source }
    }

    pub(crate) fn footer(path: impl Into<PathBuf>, reason: impl Into<String>) -> Self {
        Error::Footer {
            path: path.into(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NoInputs => write!(f, "no input files"),
            Error::OutputIsInput(p) => write!(f, "the output {} is one of the inputs", p.display()),
            Error::Incompatible { path, first, what } => {
                write!(
                    f,
                    "{} has other {what} than {}",
                    path.display(),
                    first.display()
                )
            }
            Error::Io { path, source } => write!(f, "{}: {source}", path.display()),
            Error::Parquet { path, source } => write!(f, "{}: {source}", path.display()),
            Error::Footer { path, reason } => {
                write!(
                    f,
                    "{}: column orders not restored: {reason}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io { source, .. } => Some(source),
            Error::Parquet { source, .. } => Some(source),
            _ => None,
        }
    }
}
