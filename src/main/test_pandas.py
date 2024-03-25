import os
import time
from pathlib import Path
import pandas as pd

DATA = Path('../data/stations_test.txt').resolve()

def timeit(func):
    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        res = func(*args, **kwargs)
        t2 = time.perf_counter()
        print(f'{func.__name__}() runtime: {(t2 - t1):.4f} seconds')
        return res
    return wrapper


@timeit
def test_pandas(path):
    if not os.path.isfile(path):
        raise Exception(f'No input file present at {path}')
    
    df = pd.read_csv(
        path, 
        sep=';', 
        header=None, 
        names=['station', 'reading'], 
        dtype={'station': 'category'}
    )

    results = df.groupby('station', observed=True) \
                .agg({'reading': ['mean', 'max', 'min']}) \
                .reset_index() \
                .sort_values(by='station')
    print(results)


if __name__ == '__main__':
    data = str(DATA)
    test_pandas(data)