---
description: "How pq-glue is released to PyPI (CI builds and uploads the wheels) and to crates.io (release-plz)."
---

# Releasing

## PyPI

Run the **Deploy Release** workflow (or `just release` locally). It:

1. bumps the version in `pyproject.toml` with `uv version --bump patch` (or `minor`, `major`);
2. sets the crate's version in `Cargo.toml` to match;
3. commits `chore(release): bump 🐍 -> vX.Y.Z` and tags `py-X.Y.Z`;
4. pushes the commit and the tag.

The tag starts the **CI** workflow, which builds the wheels and the sdist and uploads them
to PyPI with Trusted Publishing (the `pypi` environment). The repository needs a PyPI
trusted publisher for `lmmx/pq-glue`, workflow `CI.yml`, environment `pypi`.

## crates.io

`just ship-rust` runs release-plz: it updates the version and changelog, commits
`chore(release): 🦀 Upgrades`, pushes, and publishes the crate with a `pq-glue-vX.Y.Z` tag.
