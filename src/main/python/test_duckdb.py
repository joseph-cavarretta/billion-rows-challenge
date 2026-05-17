import argparse
import sys
from pathlib import Path

import duckdb as db

SCHEMA = {'station': 'VARCHAR', 'reading': 'DOUBLE'}


def test_duckdb(data_path: Path) -> None:
    """Run the DuckDB benchmark directly against the CSV file."""
    query = f"""
        SELECT station, ROUND(AVG(reading),3), MIN(reading), MAX(reading)
        FROM read_csv(
            '{data_path}',
            parallel=True,
            delim=';',
            header=false,
            columns={SCHEMA}
        )
        GROUP BY station
        ORDER BY station
    """
    conn = db.connect()
    results = conn.execute(query).fetchall()
    print(*results, sep='\n')


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Benchmark using DuckDB')
    parser.add_argument('data_file', type=Path, help='Path to stations.txt')
    return parser.parse_args()


def main() -> int:
    """Run the benchmark."""
    args = parse_args()
    try:
        if not args.data_file.is_file():
            raise FileNotFoundError(f'No input file present at {args.data_file}')

        test_duckdb(args.data_file)
        return 0
    except (ValueError, FileNotFoundError) as e:
        print(str(e))
        return 1


if __name__ == '__main__':
    sys.exit(main())
