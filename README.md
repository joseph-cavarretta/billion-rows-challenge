# Billion Rows Challenge

[![CI](https://github.com/joseph-cavarretta/billion-rows-challenge/actions/workflows/ci.yml/badge.svg)](https://github.com/joseph-cavarretta/billion-rows-challenge/actions/workflows/ci.yml)

Benchmarking data processing tools by aggregating 1 billion weather station readings.

Inspired by [Gunnar Morling's 1BRC](https://github.com/gunnarmorling/1brc), this project tests how fast common data engineering tools can process a 14GB CSV file.

## Part 1: CSV Parsing Leaderboard

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

## Part 2: Columnar Formats (Parquet + Vortex)

An extension that shifts the question from *"how fast can you parse 14GB of
CSV?"* to *"once the data is in a modern columnar format, how fast can
different engines and languages query it?"*

[Vortex](https://github.com/vortex-data/vortex) is a state-of-the-art columnar
format (LF AI & Data project) that can evaluate filters on compressed data
without fully decompressing it. As of January 2026 it's a
[core DuckDB extension](https://duckdb.org/2026/01/23/duckdb-vortex-extension).

**Why a separate leaderboard?** These numbers aren't comparable to Part 1 — the
CSV parse cost has been paid up front by a one-time conversion, so query times
drop dramatically. The interesting comparison here is *format × engine × language*
on identical hardware, plus the on-disk size of each format.

### Format sizes

| Format | Size on disk | Notes |
|--------|-------------|-------|
| CSV    | _TBD_       | baseline (Part 1) |
| Parquet (zstd) | _TBD_ | |
| Vortex | _TBD_       | |

### Query leaderboard

| Rank | Engine + Language | Format | Cold | Warm | Notes |
|------|-------------------|--------|------|------|-------|
| _?_  | DuckDB (SQL)      | Vortex | _TBD_ | _TBD_ | `read_vortex()` core extension |
| _?_  | Polars (Python)   | Vortex | _TBD_ | _TBD_ | Vortex → Arrow → Polars |
| _?_  | DataFusion (Rust) | Vortex | _TBD_ | _TBD_ | Vortex DataFusion TableProvider |

### Running Part 2

```bash
# one-time: convert stations.txt into measurements.parquet + measurements.vortex
make convert

# run the format benchmarks
make duckdb-vortex
make polars-vortex
make rust-vortex
```

> **Status:** scaffolding only. `src/scripts/convert_data.py` is implemented;
> the three benchmark entries are stubs (`raise NotImplementedError` /
> `todo!()`) with the harness and API pointers in place — fill in the
> aggregation logic and record results above. The Vortex crates and Python
> bindings are young; pin versions when you implement.

### Layout

```
src/
├── scripts/
│   ├── create_data.py     # Part 1: CSV generator
│   └── convert_data.py    # Part 2: CSV -> Parquet + Vortex
└── main/
    ├── python/
    │   ├── duckdb_vortex.py    # stub
    │   └── polars_vortex.py    # stub
    └── rust/                   # stub (cargo: vortex + datafusion)
        ├── Cargo.toml
        └── src/main.rs
```

## Author

Joseph Cavarretta
- Email: joseph.m.cavarretta@gmail.com
- [GitHub](https://github.com/joseph-cavarretta)
- [LinkedIn](https://www.linkedin.com/in/joseph-cavarretta-87242871/)
