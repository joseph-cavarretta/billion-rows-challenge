# Billion Rows Challenge

Benchmarking data processing tools by aggregating 1 billion weather station readings.

Inspired by [Gunnar Morling's 1BRC](https://github.com/gunnarmorling/1brc), this project tests how fast common data engineering tools can process a 14GB CSV file.

## Leaderboard

Results on 1 billion rows (14GB CSV).

| Rank | Tool   | Time      | Method                       |
|------|--------|-----------|------------------------------|
| 1    | DuckDB | 13.97s    | OLAP columnar database       |
| 2    | Polars | 51.43s    | Streaming lazy evaluation    |
| 3    | Python | 174.18s   | Multiprocessing              |
| 4    | Pandas | 351.40s   | Chunked DataFrame processing |
| 5    | SQLite | 1049.36s  | OLTP row-based database      |
| -    | AWK    | TBD       | Bash script                  |

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
```

## Project Status

Work in progress. AWK implementation is not yet benchmarked.

## Author

Joseph Cavarretta
- Email: joseph.m.cavarretta@gmail.com
- [GitHub](https://github.com/joseph-cavarretta)
- [LinkedIn](https://www.linkedin.com/in/joseph-cavarretta-87242871/)
