"""Benchmark using pure Python multiprocessing for the billion rows challenge."""

import argparse
import sys
from multiprocessing import Pool
from pathlib import Path

from src.scripts.logging_config import setup_logger
from src.scripts.timeit import timeit

logger = setup_logger(__name__)

DEFAULT_PARTITIONS = 12
DEFAULT_CORES = 12


def create_ranges(data_path: Path, partitions: int) -> list[tuple[int, int]]:
    """Create byte ranges for file partitioning."""
    size = data_path.stat().st_size
    chunk_size = size // partitions
    ranges: list[tuple[int, int]] = []

    for i in range(partitions):
        start = i * chunk_size
        end = size if i == partitions - 1 else (i + 1) * chunk_size
        ranges.append((start, end))

    return ranges


def process_file_partition(
    data_path: Path, start: int, end: int
) -> dict[str, list[float | int]]:
    """Process a partition of the file and return aggregated station data."""
    records: dict[str, list[float | int]] = {}

    with data_path.open("r", encoding="utf-8", newline="") as f:
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


def _process_partition_wrapper(
    args: tuple[Path, int, int]
) -> dict[str, list[float | int]]:
    """Wrapper for multiprocessing starmap compatibility."""
    return process_file_partition(*args)


@timeit
def test_python(
    data_path: Path, partitions: int, cores: int
) -> list[tuple[str, float, float, float]]:
    """Run the Python multiprocessing benchmark."""
    ranges = create_ranges(data_path, partitions)
    partition_args = [(data_path, start, end) for start, end in ranges]

    with Pool(cores) as pool:
        res = pool.map(_process_partition_wrapper, partition_args)

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


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Benchmark using pure Python multiprocessing'
    )
    parser.add_argument(
        'data_file',
        type=Path,
        help='Path to stations.txt'
    )
    parser.add_argument(
        '--partitions',
        type=int,
        default=DEFAULT_PARTITIONS,
        help=f'Number of file partitions (default: {DEFAULT_PARTITIONS})'
    )
    parser.add_argument(
        '--cores',
        type=int,
        default=DEFAULT_CORES,
        help=f'Number of CPU cores to use (default: {DEFAULT_CORES})'
    )
    return parser.parse_args()


def main() -> int:
    """Run the benchmark."""
    args = parse_args()
    try:
        if not args.data_file.is_file():
            raise FileNotFoundError(f'No input file present at {args.data_file}')

        logger.info(
            f'Processing {args.data_file} with {args.partitions} partitions '
            f'and {args.cores} cores'
        )
        results = test_python(args.data_file, args.partitions, args.cores)
        print(*results, sep='\n')
        return 0
    except (ValueError, FileNotFoundError) as e:
        logger.error(str(e))
        return 1


if __name__ == '__main__':
    sys.exit(main())
