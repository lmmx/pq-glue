"""Write nested Parquet files to concatenate, from a fixed seed.

    uv run make_data.py [--files 24] [--rows 60000] [--out data]

Each file has the shape of a Wikidata claims table (as wikidata-pq writes it): an id,
a property, a value struct, and lists of qualifier and reference structs that hold
lists of values, with nulls and empty lists throughout and a few float columns. Files
are written by pyarrow with zstd, in row groups of 122,880 rows, so a file of fewer rows
is one row group (as each chunk's file is in wikidata-pq).
"""

import argparse
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

VALUE = pa.struct(
    [
        ("type", pa.large_string()),
        ("text", pa.large_string()),
        ("amount", pa.float64()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("time", pa.large_string()),
    ]
)
SNAK = pa.struct([("property", pa.large_string()), ("value", pa.large_list(VALUE))])
SCHEMA = pa.schema(
    [
        pa.field("id", pa.large_string(), nullable=False),
        ("property", pa.large_string()),
        ("rank", pa.large_string()),
        ("value", VALUE),
        ("qualifiers", pa.large_list(SNAK)),
        ("references", pa.large_list(pa.struct([("snaks", pa.large_list(SNAK))]))),
    ]
)


def value(rng: random.Random) -> dict | None:
    """One claim value: an item, a string, a quantity, a coordinate or a time."""
    kind = rng.choice(["item", "string", "quantity", "coordinate", "time", None])
    if kind is None:
        return None
    v = {
        "type": kind,
        "text": None,
        "amount": None,
        "latitude": None,
        "longitude": None,
        "time": None,
    }
    if kind in ("item", "string"):
        v["text"] = (
            f"Q{rng.randrange(10**8)}"
            if kind == "item"
            else "".join(rng.choices("abcdefghij ", k=rng.randrange(4, 40)))
        )
    elif kind == "quantity":
        v["amount"] = rng.choice([rng.uniform(-1e6, 1e6), 0.0, -0.0, float("nan")])
    elif kind == "coordinate":
        v["latitude"], v["longitude"] = rng.uniform(-90, 90), rng.uniform(-180, 180)
    else:
        v["time"] = (
            f"+{rng.randrange(1000, 2030)}-{rng.randrange(1, 13):02d}-00T00:00:00Z"
        )
    return v


def snaks(rng: random.Random, n: int) -> list:
    """`n` snaks of a random property with one to three values."""
    return [
        {
            "property": f"P{rng.randrange(9000)}",
            "value": [value(rng) for _ in range(rng.randrange(1, 4))],
        }
        for _ in range(n)
    ]


def table(i: int, rows: int) -> pa.Table:
    """File `i`'s rows."""
    rng = random.Random(i)
    cols: dict[str, list] = {name: [] for name in SCHEMA.names}
    for r in range(rows):
        cols["id"].append(f"Q{i * rows + r}")
        cols["property"].append(f"P{rng.randrange(9000)}")
        cols["rank"].append(rng.choice(["normal", "preferred", "deprecated"]))
        cols["value"].append(value(rng))
        cols["qualifiers"].append(
            None if r % 9 == 0 else snaks(rng, rng.choice([0, 0, 1, 2, 3]))
        )
        refs = rng.choice([0, 1, 1, 2])
        cols["references"].append(
            [{"snaks": snaks(rng, rng.randrange(1, 4))} for _ in range(refs)]
        )
    return pa.table(cols, schema=SCHEMA)


def main() -> None:
    """Write the files."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--files", type=int, default=24)
    parser.add_argument("--rows", type=int, default=60_000)
    parser.add_argument("--out", type=Path, default=Path("data"))
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for i in range(args.files):
        path = args.out / f"part_{i:04d}.parquet"
        pq.write_table(
            table(i, args.rows), path, compression="zstd", row_group_size=122_880
        )
        print(f"{path}: {path.stat().st_size / 2**20:.1f} MB", flush=True)


if __name__ == "__main__":
    main()
