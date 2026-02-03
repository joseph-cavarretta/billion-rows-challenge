## Overview
In January 2024, [Gunnar Morling](https://github.com/gunnarmorling) released a project called the [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc), which was a call to developers to push the limits of Java and see how fast they could process and aggregate 1 billion rows of data.

Inspired by the idea of pushing a language to its performance limits, I wanted to try some common data processing tools used in the DE field to test my own skills.

The basic premise of this challenge is to take a csv file containing 1 billion weather readings for stations all around the world, and find the min, max, and mean reading for each station as fast as possible.

The catch in my case is that the data file is larger than my RAM. So processing the full dataset in memory is not an option.

## Languages / Tools Tested:
**PYTHON** - Multi-threaded on 12-cores
  
**PANDAS** - Using chunking on a dataframe to process the file in batches

**POLARS** - Multi-threaded by default and allows for streaming processing on datasets larger than available memory

**DUCKDB** - An OLAP database that utilizes a columnar-vectorized query execution engine which supports larger-than-memory processing

**SQLITE** - A local OLTP database native to Python

## Test Details:
:pencil2:&emsp; Tests were carried out on a 14GB csv file \
:pencil2:&emsp; Each test consisted of reading, aggregating, and returning formatted results \
:pencil2:&emsp; The test file contains data in the following format: `station_name;temperature_reading` (to one decimal point) \
:pencil2:&emsp; The program must find the min, max, and mean temperature readings for each station and return sorted results

## Machine Details:
:computer:&emsp; Dell Inspiron 5579 \
:computer:&emsp; 8x Intel Core i7-8550U Processors \
:computer:&emsp; 12GB Memory \
:computer:&emsp; Ubuntu 22.10 64-bit

## To Run:
:clock130:&emsp; Setup your local python virtual environment and install `requirements.txt` \
:clock130:&emsp; Create the data file by running `make create_data` \
:clock130:&emsp; This will construct the 1 billion row data file and save it as `src/data/stations.txt` \
:clock130:&emsp; Each script is written in its own file titled `test_{framework name}.py`
:clock130:&emsp; Run each script individually using the makefile commands (`make python`, `make polars`, 'make sqlite`, etc)

## Files:
**test_python.py**:  

**test_pandas.py**: 
- Gets count of lines in file via a bash subprocess
- Read the file in 100 chunks
- Performs aggregation and sorting on chunks before joining them together and returning results

**test_polars.py**:
- Reads the file into a lazyframe
- Streams the aggregations and sorting to materialize the results in batches

**test_duckdb.py**:
- Creates database `src/db/duckdb/stations.duck_db`
- Creates table `stations` and loads from file in parallel
- Runs aggregation query and returns results

**test_sqlite.py**: 
- Creates database `src/db/sqlite3/stations.db`
- Creates table `stations` and loads data file
- Runs aggregation query and returns results
- Note: using an index on `stations.station` removed `--USE TEMP B-TREE FOR GROUP BY` from the query plan but did not improve query speed (it slowed it by 3-4x). The index also increased db size from 26GB to 44GB - so be weary of disk space.

## Results
Pandas: 351.4027s
<p align="left">
<img width='400' src='assets/pandas.png'>
</p>
Polars: 51.4326s
<p align="left">
<img width='400' src='assets/polars.png'>
</p>
DuckDB: 13.9680s
<p align="left">
<img width='400' src='assets/duck_db.png'>
</p>
Sqlite: 1049.3571s
<p align="left">
<img width='400' src='assets/sqlite_no_index.png'>
</p>

## Developed by:
Joseph Cavarretta &ensp; | &ensp; joseph.m.cavarretta@gmail.com &ensp; | &ensp; [Github](https://github.com/joseph-cavarretta) &ensp; | &ensp; [LinkedIn](https://www.linkedin.com/in/joseph-cavarretta-87242871/)
