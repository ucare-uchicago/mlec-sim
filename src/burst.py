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



def iter(failureGenerator_: FailureGenerator, sys_, iters, mission):
    try:
        res = 0
        failureGenerator = copy.deepcopy(failureGenerator_)
        sys = copy.deepcopy(sys_)
        mytimer = Mytimer()
        repair = Repair(sys, sys.place_type)
        placement = Placement(sys, sys.place_type)

        start = time.time()
        for iter in range(0, iters):
            sim = Simulate(mission, sys.num_disks, sys, repair, placement)
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
    
    for num_failed_disks in range(1,21):
        mission = YEAR
        # failureGenerator = FailureGenerator(afr, GoogleBurst(50, 50), is_burst=True)
        failureGenerator = FailureGenerator(afr, ArbitraryBurst(num_failed_disks), is_burst=True)

        place_type = get_placement_index(placement)
        
        sys = System(total_drives, drives_per_rack, k_local, p_local, place_type, cap * 1024 * 1024,
                io_speed, 1, k_net, p_net, adapt, rack_fail = 0)

        failed_iters = 0
        total_iters = 0
        metrics = Metrics()

        # res = simulate(failureGenerator, sys, iters=100, epochs=1, concur=1, mission=mission)
        # return


        start  = time.time()
        res = simulate(failureGenerator, sys, iters=5000, epochs=200, concur=200, mission=mission)
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
        output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, prob_dl,
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
        
    
    if placement in ['RAID_NET']:
        k_local = 1
        p_local = 0

    dist = args.dist

    burst_sim(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement, dist)