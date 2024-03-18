import os
import pandas as pd

def run_pandas(path):
    if not os.path.isfile(path):
        raise Exception(f'No input file present at {path}')
    
    df = pd.read_csv(path, sep=';', header=None, names=['station', 'reading'])
    df.groupby('station').agg({'reading': ['min', 'max', 'mean']})


if __name__ == '__main__':
    test_file = '../data/stations_test.txt'
    run_pandas(test_file)