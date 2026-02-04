"""Benchmark using DuckDB for the billion rows challenge."""

import argparse
import sys
from pathlib import Path

import duckdb as db

from src.scripts.logging_config import setup_logger
from src.scripts.timeit import timeit

logger = setup_logger(__name__)

DB_DIR = Path('src/db/duckdb/').resolve()
DB_PATH = Path('src/db/duckdb/stations.duck_db').resolve()

TABLE = 'stations'
SCHEMA = {'station': 'VARCHAR', 'reading': 'DOUBLE'}
CONFIG = {'threads': 8}


def create_db() -> None:
    """Create the DuckDB database, removing existing one if present."""
    if DB_PATH.is_file():
        DB_PATH.unlink()

    DB_DIR.mkdir(parents=True, exist_ok=True)
    logger.info('Database directory created')


def connect_db() -> db.DuckDBPyConnection:
    """Connect to the DuckDB database."""
    return db.connect(str(DB_PATH), config=CONFIG)


def create_table(conn: db.DuckDBPyConnection, data_path: Path) -> None:
    """Create and populate the stations table from CSV."""
    ddl = f"""
        CREATE OR REPLACE TABLE stations AS
            SELECT *
            FROM read_csv(
                '{data_path}',
                parallel=True,
                delim=';',
                header=false,
                names={list(SCHEMA.keys())},
                columns={SCHEMA}
            )
        """
    conn.execute(ddl)
    logger.info(f'Table {TABLE} created')
    row_count = get_row_count(conn)
    logger.info(f'Table {TABLE} loaded with {row_count:,} rows')


def get_row_count(conn: db.DuckDBPyConnection) -> int:
    """Get the number of rows in the stations table."""
    query = f"""
        SELECT COUNT(1) FROM {TABLE}
    """
    conn.execute(query)
    results = conn.fetchall()
    return results[0][0]


def cleanup() -> None:
    """Remove the database file."""
    DB_PATH.unlink()
    logger.info('Database cleaned up')


@timeit
def test_duckdb(conn: db.DuckDBPyConnection) -> None:
    """Run the DuckDB benchmark query."""
    query = f"""
        SELECT station, ROUND(AVG(reading),3) , MIN(reading), MAX(reading)
        FROM {TABLE}
        GROUP BY station
        ORDER BY station
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print(*results, sep='\n')


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Benchmark using DuckDB'
    )
    parser.add_argument(
        'data_file',
        type=Path,
        help='Path to stations.txt'
    )
    return parser.parse_args()


def main() -> int:
    """Run the benchmark."""
    args = parse_args()
    try:
        if not args.data_file.is_file():
            raise FileNotFoundError(f'No input file present at {args.data_file}')

        logger.info(f'Processing {args.data_file}')
        create_db()
        conn = connect_db()
        create_table(conn, args.data_file)
        test_duckdb(conn)
        cleanup()
        return 0
    except (ValueError, FileNotFoundError) as e:
        logger.error(str(e))
        return 1


if __name__ == '__main__':
    sys.exit(main())
