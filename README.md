## Overview
In January 2024, software engineer [Gunnar Morling](https://github.com/gunnarmorling) released a project called the [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc), which was a call to developers to push the limits of Java and see how fast we could process and aggregate 1 billion rows of data.

Inspired by the idea of pushing a language to its performance limits, I wanted to try some common data storage and processing tools used in the Data Engineering field to test my own skills.

The basic premise of this challenge is to take a csv file containing 1 billion weather readings for stations all around the world, and find the min, max, and mean reading for each station as fast as possible.

### Languages / Tools Tested:
- Python
    - Multi-threaded to run in parallel on 8-cores
- Pandas
    - Using chunking on a dataframe to process the file in batches
- Polars
    - Multi-threaded by default and allows for streaming processing on datasets larger than available memory
- DuckDB
    - An OLAP database that utilizes a columnar-vectorized query execution engine that suports larger-than-memory processing
- Sqlite
    - A local OLTP database native to Python. Sqlite isn't meant for high volume analytical workloads, but was included just for comparison

### Test Details:
- Tests were carried out on a 14GB csv file
- Each test consisted of reading, aggregating, and returning formatted results
- The test file contains data in the following format: `station_name;temperature_reading` (to one decimal point)
- The program must find the min, max, and mean temperatures readings for each station and return the full results

### Machine Details:
- Dell Inspiron 5579
- 8x Intel Core i7-8550U Processors
- 12GB Memory
- Ubuntu 22.10 64-bit

## To Run:
- Setup your local python virtual environment and install `requirements.txt`
- Create the data file by running `src/scripts/create_data.py`
- This will construct the 1 billion row data file and save it as `src/data/stations.txt`
- Each script is written in its own file titled `test_{framework name}.py` and can be run individually under from `src/main`

## Results

<p align="center">
<img width='1400' alt='Dashboard' src='https://user-images.githubusercontent.com/57957983/226228269-63b9c991-44ad-478c-ac8a-7a7041cda3e7.png'>
</p>

## Developed by:
Joseph Cavarretta |
joseph.m.cavarretta@gmail.com |
[Github](https://github.com/joseph-cavarretta) |
[LinkedIn](https://www.linkedin.com/in/joseph-cavarretta-87242871/))
