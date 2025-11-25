import os
import sys
import random
import numpy as np
from pathlib import Path


RAW_DATA = Path('src/data/stations_raw.txt').resolve()
OUT_FILE = Path('src/data/stations.txt').resolve()
RECORDS = int(sys.argv[1])


def valid_record_cnt(records: int) -> bool:
    return records >=1


class CreateData:
    def __init__(self, inpath: str, outpath: str, records: int):
        self.inpath = inpath
        self.outpath = outpath
        self.stations = tuple()
        self.records = records
        self.std = 10
        self.sep = ';'


    def load_infile(self):
        """
        Loads weather stations from file with format: station;avg_temp
        """
        if not os.path.isfile(self.inpath):
            raise Exception(f'No input file present at {self.inpath}')
        else:
            with open(self.inpath) as f:
                lines = f.readlines()
                for line in lines:
                    station, avg_temp = line.strip('\n').split(';')
                    self.stations = self.stations + ((station, float(avg_temp)),)
            print(f'Raw data loaded for {len(self.stations)} stations')


    def create_record(self):
        """
        Creates a single record by randomly sampling input file
        """
        station, mean_temperature = random.choice(self.stations)
        random_temp = np.random.normal(mean_temperature, self.std)
        return station, random_temp


    def create_dataset(self):
        """
        Creates a dataset of size n (self.records) and writes to file
        """
        with open(self.outpath, 'w') as f:
            for i in range(self.records):
                station, temp = self.create_record()
                f.write(f"{station}{self.sep}{temp:.1f}\n")
        print(f'Test dataset created with {self.records:,} records')
        print(f'Test file size: {os.path.getsize(self.outpath)//1024**3} Gb')


if __name__ == '__main__':
    if valid_record_cnt(RECORDS):
        print(f'Creating {RECORDS:,} records...')
        data = CreateData(RAW_DATA, OUT_FILE, RECORDS)
        data.load_infile()
        data.create_dataset()
    else:
        print(
            f'Invalid record count: {RECORDS}. '
            f'Must be >= 1.'
        )
