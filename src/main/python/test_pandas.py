import subprocess
import warnings
from pathlib import Path

import pandas as pd

from src.scripts.timeit import timeit

warnings.simplefilter(action='ignore', category=FutureWarning)

DATA = Path('src/data/stations.txt').resolve()
CHUNKS = 100


def count_records():
    bash = f'wc -l {DATA}'
    num_lines = int(subprocess.check_output(bash, shell=True).split()[0])
    return num_lines


@timeit
def test_pandas(path: Path, lines: int) -> None:
    chunksize = lines // CHUNKS
    records = pd.DataFrame(columns = ['station', 'max', 'min', 'count', 'sum'])

    if not path.is_file():
        raise FileNotFoundError(f'No input file present at {path}')

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


if __name__ == '__main__':
    lines = count_records()
    test_pandas(DATA, lines)
