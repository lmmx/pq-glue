# Development

## Layout

| path | contents |
| --- | --- |
| `src/` | the crate: `concat.rs`, `input.rs`, `footer.rs`, `error.rs`, and `python.rs` (the extension module, feature `python`) |
| `tests/` | the Rust integration tests (`data.rs`, `metadata.rs`, `errors.rs`), their inputs written with arrow-rs |
| `python/pq_glue/` | the Python package, and `_pq_glue.pyi`, the stub of the compiled module |
| `python/tests/` | the Python tests, their inputs written with pyarrow |
| `bench/` | the benchmarks, a project of their own |
| `docs/site/` | this site; `docs/journal/` the development journal |

## Building and testing

The tools: a Rust toolchain, [uv](<https://docs.astral.sh/uv/>), [just](<https://just.systems/>), and for the lints taplo, whitespace-format and cargo-machete.

```sh
just py         # maturin develop, then pytest
just test-ci    # cargo test and the doc tests
just clippy     # clippy on every target and feature, warnings as errors
just t          # ty, the type checker
just pc         # formatting and lints (as pre-commit runs them)
```

## Commits

Commit messages follow [Conventional Commits](<https://www.conventionalcommits.org/>) (`feat:`, `fix:`, `docs:`, `ci:` ...); the `conventional-pre-commit` hook checks them. `release-plz.toml` groups the changelog by these prefixes.
