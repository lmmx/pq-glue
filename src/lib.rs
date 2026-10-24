//! Glue Parquet files together without decoding them.
//!
//! [`concat`](concat()) joins Parquet files that share one schema into one file by copying each
//! row group's encoded column chunks (pages, dictionaries, statistics) as they are:
//! nothing is decoded or re-encoded, so the cost is the bytes copied, whatever the
//! nesting.
//!
//! The output has the inputs' row groups in order, the first input's schema and
//! key-value metadata (`ARROW:schema`, which Arrow readers need for the files' Arrow
//! types), the inputs' page indexes and bloom filters where they have them, and the
//! inputs' column orders, under which their statistics were computed (see
//! [`footer::restore_column_orders`]).
//!
//! ```no_run
//! # fn main() -> pq_glue::Result<()> {
//! use std::path::{Path, PathBuf};
//!
//! let inputs = [PathBuf::from("a.parquet"), PathBuf::from("b.parquet")];
//! let rows = pq_glue::concat(&inputs, Path::new("ab.parquet"))?;
//! # Ok(())
//! # }
//! ```
//!
//! With the `parallel` feature, `concat_many` runs many such concatenations at once,
//! on rayon. With the `read` feature, [`read::read`] reads a file to Arrow record
//! batches, its row groups decoded in parallel.

mod concat;
mod error;
pub mod footer;
mod input;
#[cfg(feature = "python")]
mod python;
#[cfg(feature = "read")]
pub mod read;
#[cfg(feature = "write")]
pub mod write;

pub use concat::concat;
#[cfg(feature = "parallel")]
pub use concat::concat_many;
pub use error::{Error, Result};
