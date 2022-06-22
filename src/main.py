from concurrent.futures import ProcessPoolExecutor
import numpy as np
import math
import copy
import traceback
import os
import heapq

# Custom stuff
from drive_args import DriveArgs
from repair_queue import RepairQueue
from sys_state import SysState
from util import wait_futures
from repair import calc_dp_repair_time_serkay, calc_raid_repair_time, calc_dp_repair_time, gen_new_failures
from constants import debug, YEAR

def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)

def tick_raid(state_):
    np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
    state: SysState = copy.deepcopy(state_)
    state.gen_drives()
    
    if debug: print("Number of failures {}".format(len(state.drives[state.drives < YEAR])))
    # Filtering out failure time for each stripe, we are only interested in < 1 year
    stripe_idx = 0
    for stripe in state.drives:
        failure_times = stripe[stripe < YEAR]
        failure_idxs = np.where(stripe < YEAR)[0]
        # If this stripe does not has more than parity amount of failure, it will not fail
        if len(failure_times) <= state.dirve_args.parity_shards:
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
            if rec_queue.size() > state.dirve_args.parity_shards:
                return 1
            
            if debug: print('------------')
        stripe_idx += 1

    return 0

def tick_dp(state_):
    np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
    state: SysState = copy.deepcopy(state_)
    state.gen_drives()
    # we have all the disks that we are simulating
    # different from the original sim, this state contains only SINGLE system
    
    # we only care about the ones that are within a year
    failure_times = state.drives[state.drives < YEAR]
    failure_idxs = np.where(state.drives < YEAR)[0]

    # sort the failures by failure_time, preserve the indices so that we can use it later
    failures = list(zip(failure_times, failure_idxs))
    heapq.heapify(failures)

    #print("We have " + str(len(failures)) + " failures")

    # these are drives that are "repairing"
    rec_queue = RepairQueue()

    # system failure is only possible when failed drives are more than parity shards
    #   (because otherwise although there wont be repairs, 0 chance of failure, so we omit to save time)
    if len(failures) <= state.drive_args.parity_shards:
        return 0    

    while len(failures) != 0:
        failure_head = heapq.heappop(failures)
        fail_time = failure_head[0]
        fail_disk = failure_head[1]

        # update state
        state.fail(1)

        if debug: print("============================")
        if debug: print("Repairing disk {} that failed at {}".format(fail_disk, fail_time))
        # If the disk is not yet repaired, we keep it
        repaired = rec_queue.filter(fail_time)
        if debug: print("Repaired disks count: " + str(len(repaired)))
        # update state
        state.repair(len(repaired))

        #We generate failures for the ones that have been repaired
        if len(repaired) != 0:
            new_failure_added = gen_new_failures(failures, repaired, state)
            if debug: print("Added {} more new failures".format(new_failure_added))
        
        if debug: state.print()

        # calculate the disk repair time, this is the (priority) from SOLSim
        #  need to account the current failure
        total_failed = rec_queue.size() + 1
        
        # we repair the disk from highest priority to lowest
        for prio in list(reversed(range(1, total_failed + 1))):
            # repair each disk that has failed
            if debug: print("Repairing stripes with priority " + str(prio), flush=True)
            # reaching here means that the disk is already considered failed

            repair_time = calc_dp_repair_time(prio, state)
            # repair_time = calc_dp_repair_time_serkay(prio, state)
            
            # we need to add the repair_time to the rec_queue
            rec_queue.push((fail_time + repair_time, repair_time, fail_disk))

            # If we have stripe failures, the system fails
            if (rec_queue.size() > state.drive_args.parity_shards):
                return 1
        
        if debug: rec_queue.print()
        
    return 0

def iter(state: SysState, iters):
    try:
        res = 0
        for iter in range(0, iters):
            if state.mode == 'RAID':
                res += tick_raid(state)
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
        
    return (failed_instances, epochs * iters)

if __name__ == "__main__":
    
    for afr in range(2, 12):
        l1args = DriveArgs(d_shards=8, p_shards=1, afr=afr, drive_cap=20, rec_speed=100)
        l1sys = SysState(total_drives=50, drive_args=l1args, placement='DP')

        res = simulate(l1sys, iters=100000, epochs=24, concur=24)
        # res = simulate(l1sys, iters=1, epochs=1, concur=16)
        print('++++++++++++++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))

        if res[0] == 0:
            print("NO FAILURE!")
        else:
            nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
            print("Num of Nine: " + nn)

            output = open("s-result-{}.log".format(l1sys.mode), "a")
            output.write("{}-{}-{}: {}\n".format(l1args.data_shards, l1args.parity_shards, l1args.afr_in_pct, nn))
            output.close()

    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_afr=50&d_cap=20&dr_rw_speed=50&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&spares=0&rec_wr_spd_alloc=100