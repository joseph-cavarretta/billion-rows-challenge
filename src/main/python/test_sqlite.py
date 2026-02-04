"""Benchmark using SQLite for the billion rows challenge."""

import argparse
import sqlite3 as sql
import subprocess
import sys
from pathlib import Path

from src.scripts.logging_config import setup_logger
from src.scripts.timeit import timeit

logger = setup_logger(__name__)

DB_PATH = Path('src/db/sqlite3/stations.db').resolve()
TABLE = 'stations'
SCHEMA = {'station': 'TEXT', 'reading': 'INTEGER'}


def create_db() -> None:
    """Create the SQLite database, removing existing one if present."""
    if DB_PATH.is_file():
        DB_PATH.unlink()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    DB_PATH.touch()
    logger.info('Database created')


def connect_db() -> sql.Connection:
    """Connect to the SQLite database."""
    return sql.connect(DB_PATH)


def create_table(conn: sql.Connection) -> None:
    """Create the stations table."""
    columns = ', '.join(f'{name} {dtype}' for name, dtype in SCHEMA.items())
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            {columns}
        )"""

    conn.execute(ddl)
    logger.info(f'Table {TABLE} created')


def load_db(conn: sql.Connection, data_path: Path) -> None:
    """Load data from CSV into the database."""
    subprocess.run(
        [
            'sqlite3',
            str(DB_PATH),
            '-cmd',
            '.mode csv',
            '.separator ;',
            '.import ' + str(data_path) + ' ' + TABLE
        ],
        capture_output=True,
        check=True,
    )
    row_count = get_row_count(conn)
    logger.info(f'Table {TABLE} loaded with {row_count:,} rows')


def get_row_count(conn: sql.Connection) -> int:
    """Get the number of rows in the stations table."""
    query = f"""
        SELECT COUNT(1) FROM {TABLE}
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return results[0][0]


@timeit
def test_sqlite(conn: sql.Connection) -> None:
    """Run the SQLite benchmark query."""
    query = f"""
        SELECT station, ROUND(AVG(reading),3) , MIN(reading), MAX(reading)
        FROM {TABLE}
        GROUP BY station
        ORDER BY station
    """
    cursor = conn.cursor()
    rows = cursor.execute(query)
    results = rows.fetchall()
    print(*results, sep='\n')


def cleanup() -> None:
    """Remove the database file."""
    DB_PATH.unlink()
    logger.info('Database cleaned up')


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Benchmark using SQLite'
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
        create_table(conn)
        load_db(conn, args.data_file)
        test_sqlite(conn)
        cleanup()
        return 0
    except (ValueError, FileNotFoundError) as e:
        logger.error(str(e))
        return 1


if __name__ == '__main__':
    sys.exit(main())
