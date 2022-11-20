from concurrent.futures import ProcessPoolExecutor
import numpy as np
import math
import copy
import traceback
import logging
import random

# Custom stuff
from failure_generator import FailureGenerator, GoogleBurst
from util import wait_futures
from constants import debug, YEAR

from placement import Placement
from system import System
from repair import Repair

from simulate_burst import Simulate
from mytimer import Mytimer
from metrics import Metrics
import time

import argparse
import pandas as pd

class ArbitraryBurst:
    def __init__(self, num_fail_disks):
        self.num_fail_disks = num_fail_disks


    def gen_failure_burst(self, disks_per_rack, num_total_racks):
        failures = []
        num_disks = disks_per_rack * num_total_racks

        # we randomly sample num_fail_disks disk IDs
        diskIds = random.sample(range(num_disks), self.num_fail_disks)

        for diskId in diskIds:
            failures.append((0, diskId))        # (failure time,  failure disk id)
        return failures

# find all possible fail_disks_per_rack using backtracking algorithm
def find_all_lists(num_fail_disks, num_fail_racks, disks_per_rack):
    # print("num_fail_disks: {} num_fail_racks{} disks_per_rack{}".format(num_fail_disks, num_fail_racks, disks_per_rack))
    if num_fail_racks == 0:
        return []
    if num_fail_racks == 1:
        if num_fail_disks > disks_per_rack:
            return []
        else:
            return [[num_fail_disks]]
    # we need to make sure every rack has at least 1 disk failure
    # therefore, each rack can have up to (num_fail_disks - num_fail_racks + 1) disk failures
    # also, each rack can have at most disks_per_rack disk failures
    res = []
    for i in range(1, min(disks_per_rack, num_fail_disks - num_fail_racks + 1) + 1):
        temp_lists = find_all_lists(num_fail_disks - i, num_fail_racks - 1, disks_per_rack)
        for temp_list in temp_lists:
            temp_list.append(i)
            res.append(temp_list)
    return res



class CorrelatedBurst:
    def __init__(self, num_fail_disks, num_affect_racks, disks_per_rack):
        self.num_fail_disks = num_fail_disks
        self.num_fail_racks = num_affect_racks
        all_possible_fail_disks_per_rack = find_all_lists(num_fail_disks, num_affect_racks, disks_per_rack)
        # print("possible fail_disks_per_rack:\n {}".format(all_possible_fail_disks_per_rack))

        all_num_combinations = []
        for fail_disks_per_rack in all_possible_fail_disks_per_rack:
            num_combinations = 1
            for i in range(num_affect_racks):
                num_combinations *= math.comb(disks_per_rack, fail_disks_per_rack[i])
            all_num_combinations.append(num_combinations)
        # print("num combinations for each possible fail_disks_per_rack:\n {}".format(all_num_combinations))

        sum_combinations = sum(all_num_combinations)
        prob = [i / sum_combinations for i in all_num_combinations]
        # print("probability for each possible fail_disks_per_rack:\n {}".format(prob))
        assert (num_affect_racks <= num_fail_disks), 'have more affected racks than failed disks which is impossible!'

        self.all_possible_fail_disks_per_rack = all_possible_fail_disks_per_rack
        self.prob = prob


    def gen_failure_burst(self, disks_per_rack, num_total_racks):
        failures = []
        num_disks = disks_per_rack * num_total_racks

        # we first random choose affected racks
        rackids = random.sample(range(num_total_racks), self.num_fail_racks)

        # we first make sure every affected rack has at least one disk failure
        # we then randomly distribute the remaining disk failures among these racks
        num_fail_disks_per_rack = random.choices(self.all_possible_fail_disks_per_rack, self.prob)[0]

        for i in range(self.num_fail_racks):
            num_fail_disks_this_rack = num_fail_disks_per_rack[i]
            rackid = rackids[i]
            disk_indices_in_rack = random.sample(range(disks_per_rack), num_fail_disks_this_rack)
            for disk_index_in_rack in disk_indices_in_rack:
                diskid = disks_per_rack * rackid + disk_index_in_rack
                failures.append((0, diskid))        # (failure time,  failure disk id)

        # print((num_fail_racks, num_fail_disks))
        # print(rackids)
        # print(rack_disk_nums)
        # print(failures)
        return failures





def iter(afr, failure_list, placement, drives_per_rack, iters_list, *arg):
    try:
        results = []
        for i in range(len(failure_list)):
            num_failed_racks, num_failed_disks = failure_list[i]
            if placement == 'NET_DP':
                if num_failed_racks <= p_net:
                    res = 0
                else:
                    res = iters_list[i]
            elif num_failed_racks <= p_net:
                res = 0
            elif placement == 'RAID' and math.ceil(num_failed_disks / num_failed_racks) > p_local * (drives_per_rack // (k_local+p_local))+1:
                res = iters_list[i]
            elif num_failed_disks <= p_local:
                res = 0
            else:
                res = 0
                # start = time.time()
                failureGenerator = FailureGenerator(afr, CorrelatedBurst(num_failed_disks,num_failed_racks, drives_per_rack), is_burst=True)
                # print(time.time()-start)
                sys = System(*arg, num_disks_per_enclosure=100)
                
                mytimer = Mytimer()
                repair = Repair(sys, sys.place_type)
                placement = Placement(sys, sys.place_type)

                

                # start = time.time()
                # init_time = 0
                # sim_time = 0
                for iter in range(0, iters_list[i]):
                    # temp = time.time()
                    sim = Simulate(sys.num_disks, sys, repair, placement)
                    # init_time += time.time() - temp
                    # temp = time.time()
                    res += sim.run_simulation(failureGenerator, mytimer)
                    # sim_time += time.time() - temp
                # print(init_time)
                # print("sim time:{}".format(sim_time))
                # one_iter_time = time.time() - start
                # print(one_iter_time)
                # print(mytimer)
            results.append(res)
        return np.array(results)
        
    except Exception as e:
        print(traceback.format_exc())
        return None

# ----------------------------
# This is a parallel/multi-iter wrapper around iter() function
# We run X threads in parallel to run the simulation. X = concur.
# ----------------------------
def simulate(afr, failure_list, placement, drives_per_rack, iters_list, epochs, concur=10, *arg):
    # So tick(state) is for a single system, and we want to simulate multiple systems
    executor = ProcessPoolExecutor(concur)
    
    failed_instances = 0
    futures = []
    metrics = Metrics()

    for epoch in range(0, epochs):
        futures.append(executor.submit(iter, afr, failure_list, placement, drives_per_rack, iters_list, *arg))
    ress = wait_futures(futures)
    
    executor.shutdown(wait=False)

    combined_results = sum(ress)
    
    return combined_results



def get_placement_index(placement):
    place_type = -1
    if placement == 'RAID':
        place_type = 0
    elif placement == 'DP':
        place_type = 1
    elif placement == 'MLEC':
        place_type = 2
    elif placement == 'RAID_NET':
        place_type = 3
    elif placement == 'MLEC_DP':
        place_type = 4
    return place_type


# -----------------------------
# simulate against bursts
# -----------------------------
def burst_sim(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net,
                total_drives, drives_per_rack, placement, distribution):
    # logging.basicConfig(level=logging.INFO)
    df = pd.read_csv ('failures/exp/ornl/by_rack/burst_node_rack.csv')
    upper_bounds = {}
    # print(df)
    failure_list = []
    counts = {}
    total_count = 0
    iters_list = []
    # for index, row in df.iterrows():
    #     num_failed_racks = row['racks']
    #     num_failed_disks = row['drives']
    #     count = int(row['count'])
    #     failure_list.append((num_failed_racks, num_failed_disks))
    #     counts[(num_failed_racks, num_failed_disks)] = count
    #     total_count += count
    #     if num_failed_disks - num_failed_racks > 2 and num_failed_racks > 2 and num_failed_racks in [17,19,12]:
    #         iters_list.append(100)
    #     else:
    #         iters_list.append(100)
    print(counts)
    print(iters_list)
    # for index, row in df.iterrows():
    #     num_failed_racks = row['racks']
    #     num_failed_disks = row['drives']
    #     if num_failed_racks in upper_bounds:
    #         upper_bounds[num_failed_racks] = max(upper_bounds[num_failed_racks], num_failed_disks)
    #     else:
    #         upper_bounds[num_failed_racks] = num_failed_disks
    # for num_failed_racks in range(1,2):
    #     for num_failed_disks in range(1, 101):
    #         failure_list.append((num_failed_racks, num_failed_disks))
    # for num_failed_racks in range(2,3):
    #     for num_failed_disks in range(2, 55):
    #         failure_list.append((num_failed_racks, num_failed_disks))
    # for num_failed_racks in range(3,21):
    #     if num_failed_racks not in upper_bounds:
    #         upper_bound = 20
    #     else:
    #         upper_bound = max(upper_bounds[num_failed_racks]+1, 20)
        
    #     upper_bound = max(upper_bound, num_failed_racks+5)
    #     for num_failed_disks in range(num_failed_racks, upper_bound):
    #         failure_list.append((num_failed_racks, num_failed_disks))
    # for num_failed_racks in range(5,6):
    #     for num_failed_disks in range(5, 10):
    #         failure_list.append((num_failed_racks, num_failed_disks))
    # for num_failed_racks in range(1,21):
    #     for num_failed_disks in range(20, 21):
    #         failure_list.append((num_failed_racks, num_failed_disks))
    # for num_failed_disks in range(4,101):
    #     # for num_failed_racks in range(1, num_failed_disks+1):
    #     for num_failed_racks in range(4, 5):
    #         failure_list.append((num_failed_racks, num_failed_disks))
    #         iters_list.append(500)
    for num_failed_disks in range(1,21):
        # for num_failed_racks in range(1, num_failed_disks+1):
        for num_failed_racks in range(1, num_failed_disks+1):
            failure_list.append((num_failed_racks, num_failed_disks))
            iters_list.append(50000)

    # failure_list.append((2,4))
    # iters_list.append(10000)
    iters = 500
    epochs = 200
    place_type = get_placement_index(placement)
    start  = time.time()

    results = simulate(afr, failure_list, placement, drives_per_rack, iters_list, epochs, epochs, 
                        total_drives, drives_per_rack, k_local, p_local, place_type, cap * 1024 * 1024,
                        io_speed, 1, k_net, p_net, adapt)
    simulationTime = time.time() - start
    print("simulation time: {}".format(simulationTime))

    print(results)
    total_iters = iters * epochs
    aggr_prob = 0
    for i in range(len(failure_list)):
        failed_iters = results[i]
        num_failed_racks, num_failed_disks = failure_list[i]
        # print("num_failed_racks: {}  num_failed_disks: {}  failed_iters: {}  total_iters: {}".format(
        #                 num_failed_racks, num_failed_disks, failed_iters, total_iters))

        prob_dl = (failed_iters/(iters_list[i]*epochs))
        if failure_list[i] in counts:
            aggr_prob += prob_dl * counts[failure_list[i]] / total_count
        # print("probability of data loss: {}".format(prob_dl))
        # print()

        output = open("s-burst-{}.log".format(placement), "a")
        output.write("({}+{})({}+{}) {} {} {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_failed_racks, prob_dl,
            failed_iters, total_iters))
        output.close()
    nines = round(-math.log10(aggr_prob),3)
    print("aggr prob data loss: {} nines: {}".format(aggr_prob, nines))




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse simulator configurations.')
    parser.add_argument('-afr', type=int, help="disk annual failure rate.", default=5)
    parser.add_argument('-io_speed', type=int, help="disk repair rate (MB/s).", default=30)
    parser.add_argument('-cap', type=int, help="disk capacity (TB)", default=20)
    parser.add_argument('-adapt', type=bool, help="assume seagate adapt or not", default=False)
    parser.add_argument('-k_local', type=int, help="number of data chunks in local EC", default=7)
    parser.add_argument('-p_local', type=int, help="number of parity chunks in local EC", default=1)
    parser.add_argument('-k_net', type=int, help="number of data chunks in network EC", default=7)
    parser.add_argument('-p_net', type=int, help="number of parity chunks in network EC", default=1)
    parser.add_argument('-total_drives', type=int, help="number of total drives in the system", default=-1)
    parser.add_argument('-drives_per_rack', type=int, help="number of drives per rack", default=-1)
    parser.add_argument('-placement', type=str, help="placement policy. Can be RAID/DP/MLEC/LRC", default='MLEC')
    parser.add_argument('-dist', type=str, help="disk failure distribution. Can be exp/weibull", default='exp')
    args = parser.parse_args()

    afr = args.afr
    io_speed = args.io_speed
    cap = args.cap
    adapt = args.adapt
    k_local = args.k_local
    p_local = args.p_local
    k_net = args.k_net
    p_net = args.p_net

    total_drives = args.total_drives
    if total_drives == -1:
        total_drives = (k_local+p_local) * (k_net+p_net)


    drives_per_rack = args.drives_per_rack
    if drives_per_rack == -1:
        drives_per_rack=k_local+p_local
    
    placement = args.placement
    if placement in ['RAID', 'DP']:
        k_net = 1
        p_net = 0
        
    
    if placement in ['RAID_NET']:
        k_local = 1
        p_local = 0

    dist = args.dist

    burst_sim(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement, dist)