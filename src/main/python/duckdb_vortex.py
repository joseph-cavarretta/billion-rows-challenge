"""Benchmark: DuckDB reading a Vortex file.

STUB — the harness is done; implement `test_duckdb_vortex`.

Compare against the CSV version in `test_duckdb.py`: the query is the same
GROUP BY aggregation, the only change is the scan source (a Vortex file via
`read_vortex(...)` instead of `read_csv(...)`).
"""

import argparse
import sys
from pathlib import Path

import duckdb as db


def test_duckdb_vortex(data_path: Path) -> None:
    """Aggregate min/mean/max reading per station from a Vortex file.

    TODO(you): implement this.
      1. conn = db.connect()
      2. load the extension:  conn.execute("INSTALL vortex; LOAD vortex;")
      3. run a GROUP BY station aggregation over read_vortex('<data_path>')
         selecting station, AVG(reading), MIN(reading), MAX(reading), sorted
         by station (mirror the SQL in test_duckdb.py)
      4. fetch and print the rows, e.g. print(*rows, sep="\\n")
    """
    raise NotImplementedError("Implement the DuckDB + Vortex benchmark")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Benchmark using DuckDB + Vortex")
    parser.add_argument("data_file", type=Path, help="Path to measurements.vortex")
    return parser.parse_args()


def main() -> int:
    """Run the benchmark."""
    args = parse_args()
    try:
        if not args.data_file.exists():
            raise FileNotFoundError(f"No input file present at {args.data_file}")

        test_duckdb_vortex(args.data_file)
        return 0
    except (ValueError, FileNotFoundError, db.Error) as e:
        print(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
