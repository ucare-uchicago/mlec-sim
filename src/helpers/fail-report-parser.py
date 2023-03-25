
import json
import sys
import matplotlib.pyplot as plt
import numpy as np

with open(sys.argv[1], 'r') as f:
    data = json.load(f)

    disk_repair_percent_1 = []
    # disk_repair_percent_2 = []

    for item in data:
        curr_time = float(item['curr_time'])
        disk_infos = item['disk_infos']
        percents = []
        for disk_info in disk_infos:
            start_time = float(disk_info['repair_start_time'])
            repair_time = float(disk_info['repair_time']['0'])
            percent = (curr_time - start_time) / repair_time
            percents.append(percent)
        percents.sort()
        disk_repair_percent_1.append(percents[1])
        # disk_repair_percent_2.append(percents[2])
    print(disk_repair_percent_1)
    # print(disk_repair_percent_2)

    plt.hist(disk_repair_percent_1, bins=np.linspace(0.0, 1.0, num=11))
    plt.xlabel('Repaired percentage')
    plt.ylabel('Frequency')
    plt.title('When 2 disks fail, repaired percent of 1st disk')
    plt.savefig('hist.png')


