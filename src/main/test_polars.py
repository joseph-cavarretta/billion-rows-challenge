import os
import time
import polars as pl

# tweak as needed
# pl.Config.set_streaming_chunk_size(14000000)


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
        new_columns=['station', 'reading'],
        dtypes={'station': pl.Categorical}
    )
    
    results = df.group_by('station').agg(
            col_mean=pl.col('reading').mean().round(1), 
            col_max=pl.col('reading').max().round(1), 
            col_min=pl.col('reading').min().round(1)
    ).sort('station').collect(streaming=True)
    # .explain(streaming=True, comm_subplan_elim=False)
    print(results)


if __name__ == '__main__':
    test_file = '../data/stations_test.txt'
    print("Num Max Threads:", pl.thread_pool_size())
    test_polars(test_file)