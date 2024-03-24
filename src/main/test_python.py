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