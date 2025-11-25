from pathlib import Path
from multiprocessing import Pool


DATA = Path('src/data/stations_text.txt').resolve()
NUM_PARTITIONS = 8
CORES = 8


def get_start_positions(num_lines):
    part_size = num_lines // NUM_PARTITIONS
    start_positions = [
        i * part_size for i in range(NUM_PARTITIONS)
    ]
    return start_positions


def get_slices(start_positions):
    slices = list()
    for start, end in zip(start_positions, start_positions[1:]):
        slices.append((start, end))
    return slices


def process_file_partition(start, end):
    records = dict()

    with open(DATA, 'r') as f:
        #lines = f.read().split('\n')[start:end]

        for line in f:
            vals = line.split(';')
            station = vals[0]
            measurement = float(vals[1])

            try:
                record = records[station]
                record[0] = min(record[0], measurement)
                record[1] = max(record[1], measurement)
                record[2] += measurement
                record[3] += 1

            except KeyError:
                records[station] = [measurement, measurement, measurement, 1]

    return records


def test_python(slices):
    with Pool(CORES) as pool:
        res = pool.starmap(
            process_file_partition,
            slices
        )

    records = dict()
    
    for chunk in res:
        for station, (min_, max_, sum_, count) in chunk.items():
            try:
                record = records[station]
                record[0] = min(record[0], min_)
                record[1] = max(record[1], max_)
                record[2] += sum_
                record[3] += count
            except KeyError:
                records[station] = [min_, max_, sum_, count]

    return [
        (station, min_, max_, round(sum_ / count, 3)) for
        (station, (min_, max_, sum_, count)) in list(sorted(records.items()))
    ]



if __name__ == '__main__':
    results = test_python(slices)
    print(*results, sep='\n')
