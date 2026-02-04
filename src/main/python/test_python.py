from multiprocessing import Pool
from pathlib import Path

from src.scripts.timeit import timeit

DATA = Path('src/data/stations.txt').resolve()
PARTITIONS = 12
CORES = 12


def create_ranges(partitions: int) -> list[tuple[int, int]]:
    size = DATA.stat().st_size
    chunk_size = size // partitions
    ranges: list[tuple[int, int]] = []

    for i in range(partitions):
        start = i * chunk_size
        end = size if i == partitions - 1 else (i + 1) * chunk_size
        ranges.append((start, end))

    return ranges


def process_file_partition(start:int, end: int):
    records: dict[str, list[float | int]] = {}

    with DATA.open("r", encoding="utf-8", newline="") as f:
        f.seek(start)

        if start > 0:
            # read and discard first line
            f.readline()

        while True:
            pos = f.tell()
            if pos >= end:
                break

            line = f.readline()
            if not line:
                break

            station, measure = line.split(';', 1)
            measure = float(measure)

            s = records.get(station)
            if s is None:
                records[station] = [
                    measure, measure, measure, 1
                ]
            else:
                s[0] = min(s[0], measure)
                s[1] = max(s[1], measure)
                s[2] = s[2] + measure
                s[3] = s[3] + 1

    return records


@timeit
def test_python():
    ranges = create_ranges(PARTITIONS)

    with Pool(CORES) as pool:
        res = pool.starmap(
            process_file_partition,
            ranges
        )

    merged = res[0]

    for chunk in res[1:]:
        for station, (min_, max_, sum_, cnt_) in chunk.items():
            s = merged.get(station)
            if s is None:
                merged[station] = [min_, max_, sum_, cnt_]
            else:
                s[0] = min(min_, s[0])
                s[1] = max(max_, s[1])
                s[2] += sum_
                s[3] += 1

    return [
        (station, min_, max_, round(sum_ / cnt_, 3)) for
        station, (min_, max_, sum_, cnt_) in list(sorted(merged.items()))
    ]


if __name__ == '__main__':
    results = test_python()
    print(*results, sep='\n')
