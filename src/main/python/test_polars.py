import argparse
import sys
from pathlib import Path

import polars as pl

# streaming chunk size for polars lazy evaluation
STREAMING_CHUNK_SIZE = 10_000_000

pl.Config.set_streaming_chunk_size(STREAMING_CHUNK_SIZE)


def test_polars(path: Path) -> None:
    """Run the polars benchmark."""
    df = pl.scan_csv(
        path,
        separator=';',
        has_header=False,
        new_columns=['station', 'reading']
    ).group_by('station').agg(
        _mean=pl.mean('reading'),
        _max=pl.max('reading'),
        _min=pl.min('reading')
    ).sort('station').collect(engine='streaming')
    print(df)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Benchmark using polars'
    )
    parser.add_argument(
        'data_file',
        type=Path,
        help='Path to stations.txt'
    )
    return parser.parse_args()


def main() -> int:
    """Run the benchmark."""
    args = parse_args()
    try:
        if not args.data_file.is_file():
            raise FileNotFoundError(f'No input file present at {args.data_file}')

        test_polars(args.data_file)
        return 0
    except (ValueError, FileNotFoundError) as e:
        print(str(e))
        return 1


if __name__ == '__main__':
    sys.exit(main())
