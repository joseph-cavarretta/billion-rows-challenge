"""Benchmark: Polars aggregating data read from a Vortex file.

STUB — the harness is done; implement `test_polars_vortex`.

Polars has no native Vortex reader, so the interesting question is the handoff:
Vortex decodes to Arrow, and Polars can wrap an Arrow table zero-copy. Compare
your result against the CSV version in `test_polars.py` (same aggregation).
"""

import argparse
import sys
from pathlib import Path

import polars as pl  # noqa: F401  # used once you implement test_polars_vortex


def test_polars_vortex(path: Path) -> None:
    """Aggregate min/mean/max reading per station from a Vortex file.

    TODO(you): implement this.
      1. read the Vortex file with the `vortex` python package (vortex-data)
         and convert to an Arrow table / RecordBatchReader
         (see https://docs.vortex.dev/ for the current read API)
      2. wrap it with polars, e.g. pl.from_arrow(...)
      3. run the same group_by("station").agg(mean/max/min) + sort as
         test_polars.py, then print the frame

    Stretch goal: push the aggregation down instead of materializing the whole
    table in memory — Vortex can filter/scan compressed segments directly.
    """
    raise NotImplementedError("Implement the Polars + Vortex benchmark")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Benchmark using Polars + Vortex")
    parser.add_argument("data_file", type=Path, help="Path to measurements.vortex")
    return parser.parse_args()


def main() -> int:
    """Run the benchmark."""
    args = parse_args()
    try:
        if not args.data_file.exists():
            raise FileNotFoundError(f"No input file present at {args.data_file}")

        test_polars_vortex(args.data_file)
        return 0
    except (ValueError, FileNotFoundError) as e:
        print(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
