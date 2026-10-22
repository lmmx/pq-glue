## Precommit

Pre-commit bundles all the dependencies for CI and you can just run these and execute
the tests you need to check.

```sh
pre-commit run --all-files
```

ty runs from Astral's `ty-pre-commit` hook locally, and in the Test workflow on GitHub
(pre-commit.ci runs hooks without network access, so it skips ty).

## Justfile

To run all development checks, install the `just` task runner and:

```sh
just full
```

Requirements:

- Rust toolchain
- `uv` from Astral
- `ty` from Astral
- `echo-comment` (via cargo)
- `taplo`, `whitespace-format`, `cargo-machete`

The recipes are in `.just/`: `rust.just`, `python.just`, `quality.just`, `docs.just`,
`ship.just` and `commit.just`. To install precommit hooks run `just install-hooks` and to
run them use `just run-pc`.

## Release

See [docs/site/maintainers/releasing.md](docs/site/maintainers/releasing.md).
