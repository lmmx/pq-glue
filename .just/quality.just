install-hooks:
   pre-commit install

run-pc:
   pre-commit run --all-files

# -------------------------------------

fix-eof-ws mode="":
    #!/usr/bin/env sh
    ARGS=''
    if [ "{{mode}}" = "check" ]; then
        ARGS="--check-only"
    fi
    whitespace-format --add-new-line-marker-at-end-of-file \
          --new-line-marker=linux \
          --normalize-new-line-markers \
          --exclude ".git/|target/|dist/|site/|__pycache__/|\.pyc$|\.swp|.egg-info/|\.so$|.json$|.lock$|.parquet$|.venv/|\..*cache/" \
          $ARGS \
          .

code-quality:
    # just ty-ci
    taplo lint
    taplo format --check
    just fix-eof-ws check
    cargo machete
    cargo fmt --check --all

code-quality-fix:
    taplo lint
    taplo format
    just fix-eof-ws
    cargo machete
    cargo fmt --all
