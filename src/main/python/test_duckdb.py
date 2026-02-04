import sys
from pathlib import Path

import duckdb as db

from src.scripts.timeit import timeit

if len(sys.argv) < 2:
    print(
        "Usage: python test_duckdb.py <path-to-stations.txt>",
        file=sys.stderr
    )
    sys.exit(1)


DATA = Path(sys.argv[1]).resolve()
DB_DIR = Path('src/db/duckdb/').resolve()
DB_PATH = Path('src/db/duckdb/stations.duck_db').resolve()

TABLE = 'stations'

SCHEMA = {'station': 'VARCHAR','reading': 'DOUBLE'}
CONFIG = {'threads': 8}


def create_db():
    if DB_PATH.is_file():
        DB_PATH.unlink()

    # mkdir if first time running
    DB_DIR.mkdir(parents=True, exist_ok=True)


def connect_db():
    conn = db.connect(str(DB_PATH), config=CONFIG)
    return conn


def create_table(conn):
    ddl = f"""
        CREATE OR REPLACE TABLE stations AS
            SELECT *
            FROM read_csv(
                '{DATA}',
                parallel=True,
                delim=';',
                header=false,
                names={list(SCHEMA.keys())},
                columns={SCHEMA}
            )
        """
    conn.execute(ddl)
    print(f'Table {TABLE} created')
    row_count = get_row_count(conn)
    print(f'Table {TABLE} loaded with {row_count} rows')


def get_row_count(conn):
    query = f"""
        SELECT COUNT(1) FROM {TABLE}
    """
    conn.execute(query)
    results = conn.fetchall()
    return results[0][0]


def cleanup():
    DB_PATH.unlink()


@timeit
def test_duckdb(conn: db.DuckDBPyConnection) -> None:
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


if __name__ == '__main__':
    create_db()
    conn = connect_db()
    create_table(conn)
    test_duckdb(conn)
    cleanup()
