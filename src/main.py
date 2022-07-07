from concurrent.futures import ProcessPoolExecutor
import numpy as np
import math
import copy
import traceback
import os
import heapq
import logging

# Custom stuff
from drive_args import DriveArgs
from repair_queue import RepairQueue
from sys_state import SysState
from util import wait_futures
from repair import calc_dp_repair_time_serkay, calc_raid_repair_time, calc_dp_repair_time, gen_new_failures
from constants import debug, YEAR

from simulate import Simulate

def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)

def tick_raid(state_):
    np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
    state: SysState = copy.deepcopy(state_)
    state.drives = state.gen_failure_times_jiajun(state.total_drives)
    
    if debug: print("Number of failures {}".format(len(state.drives[state.drives < YEAR])))
    # Filtering out failure time for each stripe, we are only interested in < 1 year
    stripe_idx = 0
    for stripe in state.drives:
        failure_times = stripe[stripe < YEAR]
        failure_idxs = np.where(stripe < YEAR)[0]
        # If this stripe does not has more than parity amount of failure, it will not fail
        if len(failure_times) <= state.drive_args.parity_shards:
            continue
        
        failures = list(zip(failure_times, failure_idxs))
        if debug: print("Stripe {} has {} failures".format(stripe_idx, len(failures)))
        if debug: print(failures)
        
        heapq.heapify(failures)
        rec_queue = RepairQueue()

        while len(failures) != 0:
            if debug: print("{} failures remaining".format(len(failures)))
            # we calculate the repair time
            head = heapq.heappop(failures)
            failed_time = head[0]
            failed_disk = head[1] # this idx is relative to stripe, not the while system
            
            if debug: print("Repairing disk {} failed at {}".format(failed_disk, failed_time))
            # Remove already repaired disks
            repaired = rec_queue.filter(failed_time)
            if debug: print("{} disk repaired, {} still repairing".format(len(repaired), rec_queue.size()))
            if len(repaired) != 0:
                new_failure_added = gen_new_failures(failures, repaired, state)
                if debug: print("{} new failures are added".format(new_failure_added))

            # Repair the disks
            repair_time = calc_raid_repair_time(len(failures) + rec_queue.size(), state)
            if debug: print("Repairing disk {} will take {} days and be fixed at day {}".format(failed_disk, round(repair_time, 2), round(failed_time + repair_time, 2)))
            rec_queue.push((failed_time + repair_time, repair_time, failed_disk))

            # If we have more than parity in rec_queue, we fail
            if rec_queue.size() > state.drive_args.parity_shards:
                return 1
            
            if debug: print('------------')
        stripe_idx += 1

    return 0

def tick_dp(state_):
    # sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, num_disks, num_disks_per_server, 
    #                 k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio)
    np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
    sysstate: SysState = copy.deepcopy(state_)
    sysstate.gen_drives()
    sim = Simulate(YEAR, 1, 1, sysstate.total_drives, sysstate.total_drives,  
                    sysstate.drive_args.data_shards, sysstate.drive_args.parity_shards, 1, 1, "", sysstate.drive_args.drive_cap * 1024 * 1024, sysstate.drive_args.rec_speed, 1, 0.1)
    return sim.run_simulation(sysstate)

def tick_raid_huan(state_):
    # sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, num_disks, num_disks_per_server, 
    #                 k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio)
    np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
    sysstate: SysState = copy.deepcopy(state_)
    sysstate.gen_drives()
    sim = Simulate(YEAR, 1, 1, sysstate.total_drives, sysstate.total_drives, 
                    sysstate.drive_args.data_shards, sysstate.drive_args.parity_shards, 1, 0, "", sysstate.drive_args.drive_cap * 1024 * 1024, sysstate.drive_args.rec_speed, 1, 0.1)
    return sim.run_simulation(sysstate)

def iter(state: SysState, iters):
    try:
        res = 0
        for iter in range(0, iters):
            if state.mode == 'RAID':
                res += tick_raid_huan(state)
            else:
                res += tick_dp(state)
        return res
    except Exception as e:
        print(traceback.format_exc())
        return None

# This is a parallel/multi-iter wrapper around iter(state)
def simulate(state, iters, epochs, concur=10):
    # So tick(state) is for a single system, and we want to simulate multiple systems
    executor = ProcessPoolExecutor(concur)
    
    failed_instances = 0
    futures = []
    for epoch in range(0, epochs):
        futures.append(executor.submit(iter, state, iters))
    res = wait_futures(futures)
    
    print(res)
    executor.shutdown()
    failed_instances += np.sum(res)
        
    return [failed_instances, epochs * iters]

if __name__ == "__main__":
    logger = logging.getLogger()
    # logging.basicConfig(level=logging.INFO)


    for afr in range(5, 6):
        l1args = DriveArgs(d_shards=8, p_shards=2, afr=afr, drive_cap=20, rec_speed=100)
        l1sys = SysState(total_drives=50, drive_args=l1args, placement='DP')

        # # res = simulate(l1sys, iters=100000, epochs=24, concur=24)
        res = simulate(l1sys, iters=100000, epochs=80, concur=80)
        while res[0] < 20:
            print(res)
            temp = simulate(l1sys, iters=100000, epochs=80, concur=80)
            res[0] += temp[0]
            res[1] += temp[1]
        # res = simulate(l1sys, iters=100, epochs=1, concur=1)
        print('++++++++++++++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))

        if res[0] == 0:
            print("NO FAILURE!")
        else:
            # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
            nn = str(round(-math.log10(res[0]/res[1]),2))
            print("Num of Nine: " + nn)

            output = open("s-result-{}.log".format(l1sys.mode), "a")
            output.write("{}-{}-{}: {}\n".format(l1args.data_shards, l1args.parity_shards, l1args.afr_in_pct, nn))
            output.close()

    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_afr=50&d_cap=20&dr_rw_speed=50&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&spares=0&rec_wr_spd_alloc=100