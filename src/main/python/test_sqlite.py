import argparse
import sqlite3 as sql
import subprocess
import sys
from pathlib import Path

DB_PATH = Path('src/db/sqlite3/stations.db').resolve()
TABLE = 'stations'
SCHEMA = {'station': 'TEXT', 'reading': 'REAL'}


def create_db() -> None:
    """Create the SQLite database, removing existing one if present."""
    if DB_PATH.is_file():
        DB_PATH.unlink()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    DB_PATH.touch()


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


def load_db(data_path: Path) -> None:
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


def test_sqlite(conn: sql.Connection) -> None:
    """Run the SQLite benchmark query."""
    query = f"""
        SELECT station, ROUND(AVG(reading),3), MIN(reading), MAX(reading)
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
    if DB_PATH.is_file():
        DB_PATH.unlink()


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Benchmark using SQLite')
    sub = parser.add_subparsers(dest='command', required=True)

    load = sub.add_parser('load', help='Create DB and load CSV')
    load.add_argument('data_file', type=Path, help='Path to stations.txt')

    sub.add_parser('query', help='Run the aggregation query')
    sub.add_parser('cleanup', help='Remove the DB file')

    return parser.parse_args()


def main() -> int:
    """Run the benchmark."""
    args = parse_args()
    try:
        if args.command == 'load':
            if not args.data_file.is_file():
                raise FileNotFoundError(f'No input file present at {args.data_file}')
            create_db()
            conn = connect_db()
            create_table(conn)
            load_db(args.data_file)
        elif args.command == 'query':
            conn = connect_db()
            test_sqlite(conn)
        elif args.command == 'cleanup':
            cleanup()
        return 0
    except (ValueError, FileNotFoundError) as e:
        print(str(e))
        return 1


if __name__ == '__main__':
    sys.exit(main())
