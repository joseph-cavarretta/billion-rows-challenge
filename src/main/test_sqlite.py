import subprocess
import sqlite3 as sql

DB_PATH = '../sqlite3/'
DB_NAME = 'stations.db'
DATA = '../data/stations_test.txt'
TABLE = 'stations'
COL_1_NAME = 'station'
COL_1_TYPE = 'TEXT'
COL_2_NAME = 'reading'
COL_2_TYPE = 'INTEGER'

def connect_db():
    conn = sql.connect(DB_PATH + DB_NAME)
    return conn


def create_table(conn):
    # drop table first?
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            {COL_1_NAME} {COL_1_TYPE} PRIMARY KEY, 
            {COL_2_NAME} {COL_2_TYPE}
        )"""
    
    conn.execute(ddl)


def load_db():
    result = subprocess.run(
        [
            'sqlite3',
            DB_NAME,
            '-cmd',
            '.mode csv',
            '.separator ;',
            '.import --skip 1 ' + DATA + ' ' + TABLE
        ],
    capture_output=True
    )


def test_sqlite(conn):
    pass

