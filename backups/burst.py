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
from constants.PlacementType import PlacementType, parse_placement
from constants.time import YEAR
from constants.constants import kilo

from system import System

from simulate import Simulate
from mytimer import Mytimer
from metrics import Metrics
import time

import argparse

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
        print("num_fail_disks: {}  num_affect_racks: {}".format(num_fail_disks, num_affect_racks))
        self.num_fail_disks = num_fail_disks
        self.num_fail_racks = num_affect_racks
        all_possible_fail_disks_per_rack = find_all_lists(num_fail_disks, num_affect_racks, disks_per_rack)
        print("possible fail_disks_per_rack:\n {}".format(all_possible_fail_disks_per_rack))

        all_num_combinations = []
        for fail_disks_per_rack in all_possible_fail_disks_per_rack:
            num_combinations = 1
            for i in range(num_affect_racks):
                num_combinations *= math.comb(disks_per_rack, fail_disks_per_rack[i])
            all_num_combinations.append(num_combinations)
        print("num combinations for each possible fail_disks_per_rack:\n {}".format(all_num_combinations))

        sum_combinations = sum(all_num_combinations)
        prob = [i / sum_combinations for i in all_num_combinations]
        print("probability for each possible fail_disks_per_rack:\n {}".format(prob))
        print()
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





def iter(failureGenerator_: FailureGenerator, sys_, iters, mission):
    try:
        res = 0
        failureGenerator = copy.deepcopy(failureGenerator_)
        sys = copy.deepcopy(sys_)
        mytimer = Mytimer()

        start = time.time()
        for iter in range(0, iters):
            sim = Simulate(mission, sys.num_disks, sys)
            res += sim.run_simulation(failureGenerator, mytimer)
        return (res, mytimer, sys.metrics)
    except Exception as e:
        print(traceback.format_exc())
        return None

# ----------------------------
# This is a parallel/multi-iter wrapper around iter() function
# We run X threads in parallel to run the simulation. X = concur.
# ----------------------------
def simulate(failureGenerator, sys, iters, epochs, concur=10, mission=YEAR):
    # So tick(state) is for a single system, and we want to simulate multiple systems
    executor = ProcessPoolExecutor(concur)
    
    failed_instances = 0
    futures = []
    metrics = Metrics()

    for epoch in range(0, epochs):
        futures.append(executor.submit(iter, failureGenerator, sys, iters, mission))
    ress = wait_futures(futures)
    
    executor.shutdown()
    for res in ress:
        failed_instances += res[0]
        metrics += res[2]
    
    # logging.info("  failed_instances: {}".format(failed_instances))
    return [failed_instances, epochs * iters, metrics]


# -----------------------------
# simulate against bursts
# -----------------------------
def burst_sim(afr, io_speed, cap, adapt, k_net, p_net, k_local, p_local,
                total_drives, drives_per_rack, placement, distribution):
    # logging.basicConfig(level=logging.INFO)
    for num_failed_racks in range(2,20):
        for num_failed_disks in range(20, 21):
            mission = YEAR
            # failureGenerator = FailureGenerator(afr, GoogleBurst(50, 50), is_burst=True)
            # failureGenerator = FailureGenerator(afr, ArbitraryBurst(num_failed_disks), is_burst=True)
            failureGenerator = FailureGenerator(afr, CorrelatedBurst(num_failed_disks,num_failed_racks, drives_per_rack), is_burst=True)

            place_type = parse_placement(placement)
            
            sys = System(total_drives, drives_per_rack, k_local, p_local, place_type, cap * kilo * kilo,
                    io_speed, 1, k_net, p_net, adapt, rack_fail = 0)

            failed_iters = 0
            total_iters = 0
            metrics = Metrics()

            # res = simulate(failureGenerator, sys, iters=100, epochs=1, concur=1, mission=mission)
            # return


            start  = time.time()
            res = simulate(failureGenerator, sys, iters=500, epochs=200, concur=200, mission=mission)
            failed_iters += res[0]
            total_iters += res[1]

            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print("failed_iters: {}  failed_iters: {}".format(failed_iters, total_iters))

            total_iters *= mission/YEAR

            # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
            prob_dl = str(failed_iters/total_iters)
            if float(prob_dl) > 0:
                nines = str(round(-math.log10(failed_iters/total_iters),3))
                sigma = str(round(1/(math.log(10) * (failed_iters**0.5)),3))
            else:
                nines = 0
                sigma = 0
            

            print("probability of data loss: {}".format(prob_dl))
            print("Num of Nine: {}".format(nines))
            print("error sigma: {}".format(sigma))
            print()

            output = open("s-burst-{}.log".format(placement), "a")
            output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {}\n".format(
                k_net, p_net, k_local, p_local, total_drives,
                num_failed_disks, num_failed_racks, prob_dl,
                nines, sigma, failed_iters, total_iters, "adapt" if adapt else "notadapt"))
            output.close()




if __name__ == "__main__":
    logger = logging.getLogger()

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
        
    
    if placement in ['SLEC_NET_CP']:
        k_local = 1
        p_local = 0

    dist = args.dist

    burst_sim(afr, io_speed, cap, adapt, k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, dist)