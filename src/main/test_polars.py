import os
import time
from pathlib import Path
import polars as pl
from timeit import timeit

# tweak as needed
pl.Config.set_streaming_chunk_size(8000000)

DATA = Path('src/data/stations.txt').resolve()


def timeit(func):
    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        res = func(*args, **kwargs)
        t2 = time.perf_counter()
        print(f'{func.__name__}() runtime: {(t2 - t1):.4f} seconds')
        return res
    return wrapper


@timeit
def test_polars(path):
    if not os.path.isfile(path):
        raise Exception(f'No input file present at {path}')
    
    df = pl.scan_csv(
        path, 
        separator=';', 
        has_header=False, 
        new_columns=['station', 'reading']
    ).group_by('station').agg(
        _mean=pl.mean('reading'), 
        _max=pl.max('reading'), 
        _min=pl.min('reading')
    ).sort('station').collect(streaming=True)
    #print(df.explain(streaming=True, comm_subplan_elim=False))
    print(df)


if __name__ == '__main__':
    print("Num Max Threads:", pl.thread_pool_size())
    test_polars(DATA)