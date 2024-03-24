import os
import time
import subprocess
from pathlib import Path
import sqlite3 as sql

DB_PATH = Path('../sqlite3/stations.db').resolve()
DATA = Path('../data/stations_test.txt').resolve()
TABLE = 'stations'
COL_1_NAME = 'station'
COL_1_TYPE = 'TEXT'
COL_2_NAME = 'reading'
COL_2_TYPE = 'INTEGER'


def timeit(func):
    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        res = func(*args, **kwargs)
        t2 = time.perf_counter()
        print(f'{func.__name__}() runtime: {(t2 - t1):.4f} seconds')
        return res
    return wrapper


def create_db():
    # if db already exists, remove for testing only
    if os.path.isfile(DB_PATH):
        os.remove(DB_PATH)
    # create new instance of db
    with open(DB_PATH, 'w'): pass
    print(f'DB created at {str(DB_PATH)}')


def connect_db():
    conn = sql.connect(DB_PATH)
    return conn


def create_table(conn):
    # drop table first?
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            {COL_1_NAME} {COL_1_TYPE}, 
            {COL_2_NAME} {COL_2_TYPE}
        )"""
    
    conn.execute(ddl)
    print(f'Table {TABLE} created')


def create_index(conn):
    """test_sqlite() is ~4x slower with an index"""
    dml = f"""
    CREATE INDEX station_idx ON {TABLE}({COL_1_NAME});
    """
    conn.execute(dml)
    print(f'Index created on {TABLE}.{COL_1_NAME}')


def load_db(conn):
    stdout = subprocess.run(
        [
            'sqlite3',
            str(DB_PATH),
            '-cmd',
            '.mode csv',
            '.separator ;',
            '.import ' + str(DATA) + ' ' + TABLE
        ],
    capture_output=True
    )
    row_count = get_row_count(conn)
    print(f'Table {TABLE} loaded with {row_count} rows')


def get_row_count(conn):
    query = f"""
        SELECT COUNT(1) FROM {TABLE}
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return results[0][0]


@timeit
def test_sqlite(conn):
    query = f"""
        SELECT station, ROUND(AVG(reading),2) , MIN(reading), MAX(reading)
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
    load_db(conn)
    test_sqlite(conn)