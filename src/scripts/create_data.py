"""Generate synthetic weather station data for the billion rows challenge."""

import argparse
import random
import sys
from pathlib import Path

import numpy as np

from src.scripts.logging_config import setup_logger

logger = setup_logger(__name__)

RAW_DATA = Path('src/data/stations_raw.txt').resolve()
OUT_FILE = Path('src/data/stations.txt').resolve()


class CreateData:
    """Generator for synthetic weather station readings.

    Reads station names and average temperatures from a seed file,
    then generates random temperature readings based on normal distribution.
    """

    def __init__(self, inpath: Path, outpath: Path, records: int) -> None:
        if records < 1:
            raise ValueError(f'Record count must be >= 1, got {records}')
        self.inpath = inpath
        self.outpath = outpath
        self.stations: tuple[tuple[str, float], ...] = ()
        self.records = records
        self.std = 10
        self.sep = ';'

    def load_infile(self) -> None:
        """Load weather stations from file with format: station;avg_temp."""
        if not self.inpath.exists():
            raise FileNotFoundError(f'No input file present at {self.inpath}')

        with open(self.inpath) as f:
            for line in f:
                station, avg_temp = line.strip('\n').split(';')
                self.stations = self.stations + ((station, float(avg_temp)),)
        logger.info(f'Loaded {len(self.stations)} stations from {self.inpath.name}')

    def create_record(self) -> tuple[str, float]:
        """Create a single record by randomly sampling input file."""
        station, mean_temperature = random.choice(self.stations)
        random_temp = np.random.normal(mean_temperature, self.std)
        return station, random_temp

    def create_dataset(self) -> None:
        """Create a dataset of size n (self.records) and write to file."""
        logger.info(f'Generating {self.records:,} records...')
        with open(self.outpath, 'w') as f:
            for _ in range(self.records):
                station, temp = self.create_record()
                f.write(f"{station}{self.sep}{temp:.1f}\n")

        file_size_gb = self.outpath.stat().st_size / 1024**3
        logger.info(f'Created {self.outpath.name} ({file_size_gb:.2f} GB)')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Generate synthetic weather station data'
    )
    parser.add_argument(
        'records',
        type=int,
        help='Number of records to generate'
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        generator = CreateData(RAW_DATA, OUT_FILE, args.records)
        generator.load_infile()
        generator.create_dataset()
        return 0
    except (ValueError, FileNotFoundError) as e:
        logger.error(str(e))
        return 1


if __name__ == '__main__':
    sys.exit(main())
