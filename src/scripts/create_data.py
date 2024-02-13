import os
import sys
import random
import numpy as np

class CreateData:

    def __init__(self, inpath, outpath):
        self.inpath = inpath
        self.outpath = outpath
        self.stations = tuple()
        self.records = 1000000000
        self.std = 10
        self.sep = ';'


    def load_infile(self):
        if not os.path.isfile(self.inpath):
            raise Exception(f'No input file present at {self.inpath}')
        else:
            with open(self.inpath) as f:
                lines = f.readlines()
                for line in lines:
                    station, avg_temp = line.strip('\n').split(';')
                    self.stations = self.stations + ((station, float(avg_temp)),)
            print(f'Raw data loaded for {len(self.stations)} stations')


    def create_record(self) -> tuple:
        station, mean_temperature = random.choice(self.stations)
        random_temp = np.random.normal(mean_temperature, self.std)
        return str(station), float(random_temp) # check if this returns str, float


    def create_dataset(self):
        with open(outpath, 'w') as f:
            for i in range(self.records):
                station, temp = self.create_record()
                f.write(f"{station}{self.sep}{temp:.3f}\n")
        print(f'Created test dataset with {self.records}')



if __name__ == '__main__':
    inpath = '../data/stations_raw.txt'
    outpath = '../data/stations.txt'
    data = CreateData(inpath, outpath)
    data.load_infile()

