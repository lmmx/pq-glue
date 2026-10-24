# pq-glue (Rust)

The crate's API documentation is on [docs.rs](<https://docs.rs/pq-glue>).

| item | what it is |
| --- | --- |
| `pq_glue::concat(inputs: &[PathBuf], output: &Path) -> Result<u64>` | concatenate the inputs into `output`, returning the rows written |
| `pq_glue::concat_many(jobs: &[(Vec<PathBuf>, PathBuf)]) -> Vec<Result<u64>>` | each job's inputs concatenated into its output, as `concat` does, the jobs in parallel on rayon (feature `parallel`) |
| `pq_glue::Error` | the errors `concat` returns: `NoInputs`, `OutputIsInput`, `Incompatible`, `Io`, `Parquet`, `Footer` |
| `pq_glue::footer::restore_column_orders` | rewrite a file's footer column orders in place |

```rust
use std::path::{Path, PathBuf};

fn main() -> pq_glue::Result<()> {
    let inputs = [PathBuf::from("a.parquet"), PathBuf::from("b.parquet")];
    let rows = pq_glue::concat(&inputs, Path::new("ab.parquet"))?;
    println!("{rows} rows");
    Ok(())
}
```

The `parallel` feature adds `concat_many` (and rayon). The `python` feature builds the Python extension module; it is only for maturin.
