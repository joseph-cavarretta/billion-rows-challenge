"""Benchmark using pandas for the billion rows challenge."""

import argparse
import subprocess
import sys
import warnings
from pathlib import Path

import pandas as pd

from src.scripts.logging_config import setup_logger
from src.scripts.timeit import timeit

warnings.simplefilter(action='ignore', category=FutureWarning)

logger = setup_logger(__name__)

CHUNKS = 100


def count_records(data_path: Path) -> int:
    """Count the number of lines in the data file."""
    result = subprocess.run(
        ['wc', '-l', str(data_path)],
        capture_output=True,
        check=True
    )
    return int(result.stdout.split()[0])


@timeit
def test_pandas(path: Path, lines: int) -> None:
    """Run the pandas benchmark."""
    chunksize = lines // CHUNKS
    records = pd.DataFrame(columns = ['station', 'max', 'min', 'count', 'sum'])

    for df in pd.read_csv(
        path,
        sep=';',
        header=None,
        names=['station', 'reading'],
        dtype={'station': 'category'},
        chunksize=chunksize
    ):
        tmp = df.groupby('station', observed=True) \
                .agg({'reading': ['max', 'min', 'count', 'sum']}) \
                .droplevel(axis=1, level=0) \
                .reset_index() \
                .rename(
                    {
                        'max': '_max',
                        'min': '_min',
                        'count': '_count',
                        'sum': '_sum'
                    }
                )
        records = pd.concat([records, tmp])

    results = records.groupby('station', observed=True) \
                .agg(
                    _max=('max', 'max'),
                    _min=('min', 'min'),
                    _count=('count', 'sum'),
                    _sum=('sum', 'sum')
                    ) \
                .reset_index()

    results['_mean'] = results['_sum'] / results['_count']

    print(results[['station', '_max', '_min', '_mean']].sort_values(by='station'))


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Benchmark using pandas'
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

        logger.info(f'Counting records in {args.data_file}')
        lines = count_records(args.data_file)
        logger.info(f'Processing {lines:,} records')
        test_pandas(args.data_file, lines)
        return 0
    except (ValueError, FileNotFoundError) as e:
        logger.error(str(e))
        return 1


if __name__ == '__main__':
    sys.exit(main())
