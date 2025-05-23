import os
import time
from pathlib import Path
import duckdb as db


DB_DIR = Path('src/db/duckdb/').resolve()
DB_PATH = Path('src/db/duckdb/stations.duck_db').resolve()
DATA = Path('src/data/stations.txt').resolve()
TABLE = 'stations'
SCHEMA = {
    'station': 'VARCHAR',
    'reading': 'DOUBLE'
}
CONFIG = {
    'threads': 8
}


def timeit(func):
    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        res = func(*args, **kwargs)
        t2 = time.perf_counter()
        print(f'{func.__name__}() runtime: {(t2 - t1):.4f} seconds')
        return res
    return wrapper


def create_db():
    if os.path.isfile(DB_PATH):
        os.remove(DB_PATH)
    # if first time running, create db directory
    if not os.path.isdir(DB_DIR):
        os.makedirs(DB_DIR)


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
    os.remove(DB_PATH)


@timeit
def test_duckdb(conn):
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