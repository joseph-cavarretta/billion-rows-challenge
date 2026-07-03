# Billion Rows Challenge

[![CI](https://github.com/joseph-cavarretta/billion-rows-challenge/actions/workflows/ci.yml/badge.svg)](https://github.com/joseph-cavarretta/billion-rows-challenge/actions/workflows/ci.yml)

Benchmarking data processing tools by aggregating 1 billion weather station readings.

Inspired by [Gunnar Morling's 1BRC](https://github.com/gunnarmorling/1brc), this project tests how fast common data engineering tools can process a 14GB CSV file.

## Leaderboard

Results on 1 billion rows (14GB CSV).

| Rank | Tool   | Cold      | Warm      | Method                       |
|------|--------|-----------|-----------|------------------------------|
| 1    | Polars | 9.81s     | 9.12s     | Streaming lazy evaluation    |
| 2    | DuckDB | 12.69s    | 12.56s    | OLAP columnar database       |
| 3    | AWK    | 38.09s    | 38.34s    | mawk + GNU parallel          |
| 4    | Pandas | 116.08s   | 114.09s   | Chunked DataFrame processing |
| 5    | Python | 174.18s   | 73.54s    | Multiprocessing              |
| 6    | AWK    | 255.77s   | 255.77s   | mawk single-threaded         |
| 7    | SQLite | 496.74s   | 496.74s   | OLTP row-based database      |

## System Specs

- Dell Inspiron 5579
- 8x Intel Core i7-8550U
- 12GB RAM
- Ubuntu 22.10 64-bit

## Quick Start

```bash
# install dependencies
uv sync

# generate test data (1 billion rows)
make create_data

# run benchmarks
make duckdb
make polars
make pandas
make sqlite
make python
make awk
```

## Author

Joseph Cavarretta
- Email: joseph.m.cavarretta@gmail.com
- [GitHub](https://github.com/joseph-cavarretta)
- [LinkedIn](https://www.linkedin.com/in/joseph-cavarretta-87242871/)
