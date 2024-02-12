import os
import sys
import random
import numpy as np

class CreateData:

    def __init__(self, inpath, outpath):
        self.inpath = inpath
        self.outpath = outpath
        self.stations = tuple()


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


    def create_record(self, std) -> tuple:
        station, mean_temperature = random.choice(self.stations)
        random_temperature = np.random.normal(mean_temperature, std)
        return (station, random_temperature)


    def create_dataset(self):
        pass


if __name__ == '__main__':
    inpath = '../data/stations_raw.txt'
    outpath = '../data/stations.txt'
    data = CreateData(inpath, outpath)
    data.load_infile()




# with open('../data/stations_raw.txt') as f:
#     data = tuple()
#     lines = f.readlines()
#     for line in lines:
#         station, avg_temp = line.strip('\n').split(';')
#         data = data + ((station, float(avg_temp)),)

