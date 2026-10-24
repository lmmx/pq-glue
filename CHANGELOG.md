# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/lmmx/pq-glue/compare/v0.1.0...v0.2.0) - 2026-10-24

### <!-- 1 -->Features

- read_parquet, a file's row groups decoded in parallel to Arrow (the read feature) ([#4](https://github.com/lmmx/pq-glue/pull/4))
- concat_many, many concatenations in one call; small inputs read whole ([#3](https://github.com/lmmx/pq-glue/pull/3))
- write Arrow data to Parquet (polars-genson's writer, moved) with native, parallel and pyarrow engines ([#2](https://github.com/lmmx/pq-glue/pull/2))
- the output keeps the inputs' created_by, format version and sorting columns ([#1](https://github.com/lmmx/pq-glue/pull/1))

### <!-- 7 -->Build System and CI

- the crate published to crates.io by CI with Trusted Publishing, its changelog written by just release (release-plz) ([#5](https://github.com/lmmx/pq-glue/pull/5))

### <!-- 9 -->Other

- ship recipe's last message prints instead of running gh, taplo skips target/

## [0.1.0](https://github.com/lmmx/pq-glue/releases/tag/v0.1.0) - 2026-10-23

### <!-- 1 -->Features

- concatenate Parquet files by copying their encoded row groups

### <!-- 4 -->Documentation

- benchmarks without the pipeline section
- benchmarks page
- plain headings
- logo of row-group planes from two axes settling into one order within a lattice
- logo of two blocks seated in order within a lattice cube
- isometric logo, two stacks of row groups joined in order on a construction grid
- minimal logo, two blocks and the two joined in order
- logo of two halves joined into a ring, indigo and amber palette
- home page hero and logo, runnable examples with hidden setup, wend's hero styles
- documentation site (zensical, GitHub Pages with PR previews, from wend), journal and development guides
- rewrite the README in plain terms, benchmark moved to bench/
- README with usage, guarantees and benchmark; numpy docstrings

### <!-- 5 -->Refactor

- split the crate into concat, input, footer, error and python modules

### <!-- 6 -->Testing

- python/tests is a package, so its conftest and the docs' can run in one session
- split the Python tests into data, metadata and errors modules with a conftest

### <!-- 7 -->Build System and CI

- ship-rust-and-python recipe ships one commit to crates.io and PyPI
- *(taplo)* format zensical.toml without reordering its nav
- *(pre-commit)* skip ty on pre-commit.ci (no network for its uv check), name tests test_*.py
- wheels and PyPI release, Deploy Release, and a test workflow (Rust, Python versions, ty)
- Justfile in .just modules, pre-commit, taplo, release-plz and dependabot, from polars-genson

### <!-- 9 -->Other

- loosen ty version
- *(lint)* docstrings for ruff, unambiguous rustdoc links, py-dev runs maturin from .venv
- a reproducible benchmark against Polars and pyarrow, with its own pyproject
- Initial commit
