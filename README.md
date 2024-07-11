## Overview
In January 2024, software engineer [Gunnar Morling](https://github.com/gunnarmorling) released a project called the [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc), which was a call to developers to push the limits of Java and see how fast we could process and aggregate 1 billion rows of data.

Inspired by the idea of pushing a language to its performance limits, I wanted to try some common data storage and processing tools used in the Data Engineering field to test my own skills.

The basic premise of this challenge is to take a csv file containing 1 billion weather readings for stations all around the world, and find the min, max, and mean reading for each station as fast as possible.

## Languages / Tools Tested:
**PYTHON** - Multi-threaded to run in parallel on 8-cores
  
**PANDAS** - Using chunking on a dataframe to process the file in batches

**POLARS** - Multi-threaded by default and allows for streaming processing on datasets larger than available memory

**DUCKDB** - An OLAP database that utilizes a columnar-vectorized query execution engine that suports larger-than-memory processing

**SQLITE** - A local OLTP database native to Python. Sqlite isn't meant for high volume analytical workloads, but was included just for comparison

## Test Details:
:pencil2:&emsp; Tests were carried out on a 14GB csv file \
:pencil2:&emsp; Each test consisted of reading, aggregating, and returning formatted results \
:pencil2:&emsp; The test file contains data in the following format: `station_name;temperature_reading` (to one decimal point) \
:pencil2:&emsp; The program must find the min, max, and mean temperatures readings for each station and return the full results

## Machine Details:
:computer:&emsp; Dell Inspiron 5579 \
:computer:&emsp; 8x Intel Core i7-8550U Processors \
:computer:&emsp; 12GB Memory \
:computer:&emsp; Ubuntu 22.10 64-bit

## To Run:
:clock130:&emsp; Setup your local python virtual environment and install `requirements.txt` \
:clock130:&emsp; Create the data file by running `src/scripts/create_data.py` \
:clock130:&emsp; This will construct the 1 billion row data file and save it as `src/data/stations.txt` \
:clock130:&emsp; Each script is written in its own file titled `test_{framework name}.py` and can be run individually under from `src/main`

## Results
Python: 000 \
Pandas: 351.4027s
<p align="left">
<img width='400' alt='Dashboard' src='https://github.com/joseph-cavarretta/photos/blob/main/test_pandas.png'>
</p>
Polars: 51.4326s
<p align="left">
<img width='400' alt='Dashboard' src='https://github.com/joseph-cavarretta/photos/blob/main/test_polars.png'>
</p>
DuckDB: 000
Sqlite: 000

## Developed by:
Joseph Cavarretta &ensp; | &ensp; joseph.m.cavarretta@gmail.com &ensp; | &ensp; [Github](https://github.com/joseph-cavarretta) &ensp; | &ensp; [LinkedIn](https://www.linkedin.com/in/joseph-cavarretta-87242871/)
