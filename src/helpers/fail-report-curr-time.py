
import json
import sys
import matplotlib.pyplot as plt
import numpy as np

with open(sys.argv[1], 'r') as f:
    data = json.load(f)

    curr_time_samples = []
    # disk_repair_percent_2 = []

    for item in data:
        curr_time = float(item['curr_time'])
        curr_time_samples.append(curr_time)
        # disk_repair_percent_2.append(percents[2])
    # print(disk_repair_percent_2)

    plt.hist(curr_time_samples, bins=np.linspace(0.0, 365, num=11))
    plt.xlabel('trigger time')
    plt.ylabel('Frequency')
    plt.title('frequency of failure trigger time')
    plt.savefig('hist.png')


