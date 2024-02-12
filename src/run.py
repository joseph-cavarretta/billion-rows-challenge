"""
Pandas (if cant fit all in memory, can we do this line by line in the CSV?)
Numpy
Polars
Pyspark
Python Standard (iterate line by line to dict?)
"""

"""
Parse input args and run script for specific framework
"""

import pandas as pd
from timeit import timeit

@timeit
def run_pandas():
    data = './data/weather_stations.csv'
    df = pd.read_csv(data, sep=';', header=None, skiprows=2)
    print(df.shape) 

run_pandas()