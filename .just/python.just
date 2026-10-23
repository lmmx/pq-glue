ruff-check mode="":
   ruff check . {{mode}}

ruff-fix:
   just ruff-check --fix

ruff-fmt:
   ruff format .

# Type checking
ty *args:
   #!/usr/bin/env bash
   ty check --exit-zero {{args}} 2> >(grep -v "WARN ty is pre-release software" >&2)

t:
   just ty --output-format=concise

# -------------------------------------

py: py-dev py-test

# Develop the Python package (debug mode), in .venv with its dependency groups
py-dev:
    uv sync --no-install-project
    uv run --no-sync maturin develop --uv

# Develop the Python package (release mode), in .venv with its dependency groups
py-release:
    uv sync --no-install-project
    uv run --no-sync maturin develop --uv --release

# Test the Python package with pytest
py-test *args:
    #!/usr/bin/env bash
    $(uv python find) -m pytest {{args}}

# -------------------------------------

setup:
   #!/usr/bin/env bash
   uv venv
   source .venv/bin/activate
   uv sync

sync:
   uv sync
