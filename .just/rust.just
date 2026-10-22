build:
    cargo build

check:
    cargo check --all-targets

check-py:
    cargo check --features python

# -------------------------------------

clippy:
    cargo clippy --all-targets --all-features --target-dir target/clippy-all-features -- -D warnings

# -------------------------------------

test *args:
    cargo nextest run {{args}}

test-ci *args:
    #!/usr/bin/env -S echo-comment --color bright-green
    # 🏃 Running Rust tests...
    cargo test {{args}}

    # 📚 Running documentation tests...
    cargo test --doc {{args}}

# -------------------------------------

docs:
    cargo doc --all-features --no-deps --document-private-items --keep-going

# -------------------------------------

clean:
    cargo clean
