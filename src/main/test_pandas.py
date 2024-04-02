import os
import subprocess
import time
from pathlib import Path
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


DATA = Path('../data/stations.txt').resolve()
CHUNKS = 100

def timeit(func):
    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        res = func(*args, **kwargs)
        t2 = time.perf_counter()
        print(f'{func.__name__}() runtime: {(t2 - t1):.4f} seconds')
        return res
    return wrapper


def count_records():
    bash = f'wc -l {DATA}'
    num_lines = int(subprocess.check_output(bash, shell=True).split()[0])
    return num_lines


@timeit
def test_pandas(path, lines):
    chunksize = lines // CHUNKS
    records = pd.DataFrame(columns = ['station', 'max', 'min', 'count', 'sum'])

    if not os.path.isfile(path):
        raise Exception(f'No input file present at {path}')
    
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