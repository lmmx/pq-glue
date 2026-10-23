"""Concatenate Parquet files with Polars, pyarrow and pq-glue, and time each.

    uv run run.py [--files 'data/*.parquet'] [--rounds 3]

Each method runs in a fresh process, the methods in a different order each round. For
each run: wall time, CPU time (user + system) and peak RSS (VmHWM, so Linux only) of that process, and the
output's size. The three outputs of the first round are checked to have the same rows
in the same order (row count and two sums of position-seeded row hashes, read with
Polars). Results are printed and appended to results.jsonl.
"""

import argparse
import glob
import json
import platform
import random
import subprocess
import sys
import tempfile
import time
from pathlib import Path

METHODS = {
    "polars": "import polars as pl; pl.scan_parquet(src).sink_parquet(dst)",
    "pyarrow": (
        "import pyarrow.parquet as pq\n"
        "with pq.ParquetWriter(dst, pq.read_schema(src[0]), compression='zstd') as w:\n"
        "    for p in src:\n"
        "        f = pq.ParquetFile(p)\n"
        "        for i in range(f.metadata.num_row_groups):\n"
        "            w.write_table(f.read_row_group(i))"
    ),
    "pq-glue": "import pq_glue; pq_glue.concat(src, dst)",
}

# Run in the child: the method, then its wall and CPU time and peak RSS as JSON
CHILD = """
import json, sys, time
src, dst = json.loads(sys.argv[1]), sys.argv[2]
t, c = time.perf_counter(), time.process_time()
{code}
# VmHWM, not ru_maxrss: on Linux ru_maxrss carries over the parent's peak across exec
hwm = next(int(l.split()[1]) for l in open("/proc/self/status") if l.startswith("VmHWM"))
print(json.dumps({{"wall": time.perf_counter() - t, "cpu": time.process_time() - c,
                  "peak_rss_mb": hwm // 1024}}))
"""


def fingerprint(path: str) -> tuple:
    """Fingerprint a Parquet file's rows and their order.

    The row count and two sums of row hashes seeded by position: equal for the same rows
    in the same order.
    """
    import polars as pl

    row = pl.struct(pl.all())
    lf = pl.scan_parquet(path).with_row_index("_row")
    exprs = [
        pl.len().alias("rows"),
        row.hash(seed=0).sum().alias("h0"),
        row.hash(seed=1).sum().alias("h1"),
    ]
    return lf.select(exprs).collect(engine="streaming").row(0)


def run(method: str, src: list[str], dst: str) -> dict:
    """One method in a fresh process."""
    code = CHILD.format(code=METHODS[method])
    out = subprocess.run(
        [sys.executable, "-c", code, json.dumps(src), dst],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def main() -> None:
    """Run every method `--rounds` times and report."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--files",
        default="data/*.parquet",
        help="a glob of the files to concatenate, in sorted order",
    )
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--results", type=Path, default=Path("results.jsonl"))
    args = parser.parse_args()
    src = sorted(glob.glob(args.files))
    if not src:
        sys.exit(f"no files match {args.files}: run make_data.py first")
    size = sum(Path(p).stat().st_size for p in src)
    print(f"{len(src)} files, {size / 2**20:.1f} MB")
    runs = []
    with tempfile.TemporaryDirectory(dir=".") as tmp:
        for r in range(args.rounds):
            order = list(METHODS)
            random.Random(r).shuffle(order)
            prints = {}
            for method in order:
                dst = str(Path(tmp) / f"{method}.parquet")
                res = run(method, src, dst) | {
                    "method": method,
                    "round": r,
                    "output_mb": Path(dst).stat().st_size / 2**20,
                }
                runs.append(res)
                print(
                    f"round {r} {method:8s} {res['wall']:7.2f} s wall {res['cpu']:7.2f} s CPU {res['peak_rss_mb']:6d} MB peak RSS",
                    flush=True,
                )
                if r == 0:
                    prints[method] = fingerprint(dst)
                Path(dst).unlink()
            if r == 0 and len(set(prints.values())) != 1:
                sys.exit(f"the outputs differ: {prints}")
    print("\nmethod    wall (s)        CPU (s)         peak RSS (MB)")
    for method in METHODS:
        mine = [x for x in runs if x["method"] == method]
        span = lambda k, f: f"{f(min(x[k] for x in mine))}-{f(max(x[k] for x in mine))}"
        print(
            f"{method:8s}  {span('wall', '{:.2f}'.format):14s}  {span('cpu', '{:.2f}'.format):14s}  {span('peak_rss_mb', str)}"
        )
    record = {
        "when": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "files": len(src),
        "bytes": size,
        "runs": runs,
    }
    with args.results.open("a") as f:
        f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    main()
