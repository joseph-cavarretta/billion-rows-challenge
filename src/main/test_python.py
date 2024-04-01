# read file in chuncks and multithread each to a processor
# for station in stations:
# station.readings_cnt = src + current
# station.readings_sum = srs + current
# station.min = curr if curr < min else min
# station.max = curr if curr > max else max

# try:
#     item = station_measurements[station]
#     item[0] = min(item[0], min_)
#     item[1] = max(item[1], max_)
#     item[2] += sum_
#     item[3] += count
# except KeyError: # first time entering it 
#     station_measurements[station] = [min_, max_, sum_, count]

import os
import time
import subprocess
from pathlib import Path
from multiprocessing import Pool

DATA = Path('../data/stations_test.txt').resolve()
NUM_PARTITIONS = 8
CORES = 8


def timeit(func):
    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        res = func(*args, **kwargs)
        t2 = time.perf_counter()
        print(f'{func.__name__}() runtime: {(t2 - t1):.4f} seconds')
        return res
    return wrapper


def process_file_partition():
    pass


def count_records():
    bash = f'wc -l {DATA}'
    num_lines = int(subprocess.check_output(bash, shell=True).split()[0])
    return num_lines


def get_start_positions(num_lines):
    part_size = num_lines // NUM_PARTITIONS
    start_positions = [
        i * part_size for i in range(NUM_PARTITIONS)
    ]
    return start_positions


def test_python():
    pass


if __name__ == '__main__':
    # with open(DATA, "r") as f:
    #     lines = f.read().split('\n')[:10]
    num_lines = count_records()
    get_start_positions(num_lines)