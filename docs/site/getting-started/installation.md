---
description: "Install pq-glue from PyPI or crates.io."
---

# Installation

## Python

pq-glue needs Python 3.10 or later. It has no Python dependencies.

```sh
pip install pq-glue
```

or with uv:

```sh
uv add pq-glue
```

Wheels are built for Linux (x86_64, aarch64, armv7), macOS (x86_64, arm64) and Windows
(x64). Each wheel uses the stable ABI (abi3), so one wheel covers every Python version
from 3.10.

## Rust

```sh
cargo add pq-glue
```

The crate depends only on the `parquet` crate, with no compression codecs: it never
decompresses a page.
