"""What concat refuses, and the exceptions it raises."""

import pq_glue
import pyarrow.parquet as pq
import pytest
from .conftest import table


def test_other_schema_rejected(tmp_path, inputs):
    """An input with another schema is rejected, and no output is left."""
    other = tmp_path / "other.parquet"
    pq.write_table(table(0, 10).drop_columns(["n"]), other)
    out = tmp_path / "out.parquet"
    with pytest.raises(ValueError, match="other schema"):
        pq_glue.concat([*inputs, str(other)], out)
    assert not out.exists()


def test_other_metadata_rejected(tmp_path, inputs):
    """An input with other key-value metadata is rejected."""
    other = tmp_path / "other.parquet"
    pq.write_table(table(0, 10).replace_schema_metadata({"origin": "elsewhere"}), other)
    with pytest.raises(ValueError, match="key-value metadata"):
        pq_glue.concat([inputs[0], str(other)], tmp_path / "out.parquet")


def test_no_inputs(tmp_path):
    """No inputs is an error."""
    with pytest.raises(ValueError, match="no input"):
        pq_glue.concat([], tmp_path / "out.parquet")


def test_output_is_input(inputs):
    """The output may not be one of the inputs."""
    with pytest.raises(ValueError, match="one of the inputs"):
        pq_glue.concat(inputs, inputs[1])


def test_missing_input(tmp_path, inputs):
    """An input that cannot be opened raises OSError."""
    with pytest.raises(OSError):
        pq_glue.concat(
            [*inputs, str(tmp_path / "missing.parquet")], tmp_path / "out.parquet"
        )


def test_not_parquet(tmp_path, inputs):
    """An input that is not Parquet raises ValueError."""
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not parquet at all")
    with pytest.raises(ValueError):
        pq_glue.concat([inputs[0], str(bad)], tmp_path / "out.parquet")
