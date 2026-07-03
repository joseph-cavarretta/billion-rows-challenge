"""Convert the CSV weather data into columnar formats (Parquet + Vortex).

This is a one-time preprocessing step for "Part 2" of the challenge, which
compares columnar file formats and query engines rather than raw text parsing.

Conversion is done with DuckDB: as of the January 2026 release, Vortex is a
core DuckDB extension, so DuckDB can read the CSV and COPY it out to both
Parquet and Vortex without any extra Python dependencies.

Note: the `vortex` extension is downloaded on first use, so this needs network
access the first time it runs (unlike the pure-CSV benchmarks).
"""

import argparse
import sys
from pathlib import Path

import duckdb as db

from src.scripts.logging_config import setup_logger

logger = setup_logger(__name__)

CSV_SCHEMA = {"station": "VARCHAR", "reading": "DOUBLE"}

PARQUET_OUT = Path("src/data/measurements.parquet").resolve()
VORTEX_OUT = Path("src/data/measurements.vortex").resolve()


class ConvertData:
    """Convert the semicolon-delimited CSV into Parquet and Vortex files."""

    def __init__(self, csv_path: Path) -> None:
        if not csv_path.is_file():
            raise FileNotFoundError(f"No input file present at {csv_path}")
        self.csv_path = csv_path
        self.conn = db.connect()

    def _read_csv_expr(self) -> str:
        """SQL expression that scans the source CSV with the fixed schema."""
        return (
            f"read_csv('{self.csv_path}', delim=';', header=false, "
            f"columns={CSV_SCHEMA})"
        )

    def to_parquet(self, out_path: Path) -> None:
        """Write the data to a zstd-compressed Parquet file."""
        logger.info(f"Writing Parquet to {out_path.name}...")
        self.conn.execute(
            f"COPY (FROM {self._read_csv_expr()}) TO '{out_path}' "
            f"(FORMAT parquet, COMPRESSION zstd)"
        )
        self._log_size(out_path)

    def to_vortex(self, out_path: Path) -> None:
        """Write the data to a Vortex file via the DuckDB core extension."""
        logger.info("Loading DuckDB vortex extension...")
        self.conn.execute("INSTALL vortex; LOAD vortex;")
        logger.info(f"Writing Vortex to {out_path.name}...")
        self.conn.execute(
            f"COPY (FROM {self._read_csv_expr()}) TO '{out_path}' (FORMAT vortex)"
        )
        self._log_size(out_path)

    @staticmethod
    def _log_size(path: Path) -> None:
        size_gb = path.stat().st_size / 1024**3
        logger.info(f"Created {path.name} ({size_gb:.2f} GB)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert CSV weather data to Parquet and Vortex"
    )
    parser.add_argument("data_file", type=Path, help="Path to stations.txt")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        converter = ConvertData(args.data_file)
        converter.to_parquet(PARQUET_OUT)
        converter.to_vortex(VORTEX_OUT)
        return 0
    except (FileNotFoundError, db.Error) as e:
        logger.error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
