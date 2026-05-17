import argparse
import sys
import warnings
from pathlib import Path

import pandas as pd

warnings.simplefilter(action='ignore', category=FutureWarning)

CHUNK_SIZE = 10_000_000


def test_pandas(path: Path) -> None:
    """Run the pandas benchmark."""
    partials: list[pd.DataFrame] = []

    for df in pd.read_csv(
        path,
        sep=';',
        header=None,
        names=['station', 'reading'],
        dtype={'station': 'category', 'reading': 'float32'},
        chunksize=CHUNK_SIZE,
    ):
        partials.append(
            df.groupby('station', observed=True)
              .agg(_max=('reading', 'max'),
                   _min=('reading', 'min'),
                   _sum=('reading', 'sum'),
                   _count=('reading', 'count'))
              .reset_index()
        )

    combined = pd.concat(partials, ignore_index=True)
    results = (
        combined.groupby('station', observed=True)
                .agg(_max=('_max', 'max'),
                     _min=('_min', 'min'),
                     _sum=('_sum', 'sum'),
                     _count=('_count', 'sum'))
                .reset_index()
    )
    results['_mean'] = results['_sum'] / results['_count']

    print(results[['station', '_max', '_min', '_mean']].sort_values(by='station'))


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Benchmark using pandas')
    parser.add_argument('data_file', type=Path, help='Path to stations.txt')
    return parser.parse_args()


def main() -> int:
    """Run the benchmark."""
    args = parse_args()
    try:
        if not args.data_file.is_file():
            raise FileNotFoundError(f'No input file present at {args.data_file}')

        test_pandas(args.data_file)
        return 0
    except (ValueError, FileNotFoundError) as e:
        print(str(e))
        return 1


if __name__ == '__main__':
    sys.exit(main())
