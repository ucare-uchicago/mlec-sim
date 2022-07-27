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
from sys_state import SysState
from util import wait_futures
from constants import debug, YEAR

from placement import Placement
from trinity import Trinity
from repair import Repair

from simulate import Simulate
from mytimer import Mytimer
import time

def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)

def tick_dp(state_, mytimer: Mytimer, sys, repair, placement):
    # sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, num_disks, num_disks_per_server, 
    #                 k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio)
    np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
    sysstate: SysState = copy.deepcopy(state_)
    sysstate.gen_drives()
    sim = Simulate(YEAR, sysstate.total_drives, sys, repair, placement)
    return sim.run_simulation(sysstate)

def tick_raid_huan(state_, mytimer: Mytimer, sys, repair, placement):
    # sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, num_disks, num_disks_per_server, 
    #                 k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio)
    np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
    sysstate: SysState = copy.deepcopy(state_)
    sysstate.gen_drives()
    sim = Simulate(YEAR, sysstate.total_drives, sys, repair, placement)
    return sim.run_simulation(sysstate)

def tick_mlec_raid(sysstate, mytimer: Mytimer, sys, repair, placement):
    # sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, num_disks, num_disks_per_server, 
    #                 k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio)
    sim = Simulate(YEAR, sysstate.total_drives, sys, repair, placement)
    return sim.run_simulation(sysstate, mytimer)

def iter(state_: SysState, iters):
    
    try:
        res = 0
        sysstate = copy.deepcopy(state_)
        mytimer = Mytimer()
        sys = Trinity(sysstate.total_drives, sysstate.drives_per_server, sysstate.drive_args.data_shards,
                sysstate.drive_args.parity_shards, sysstate.place_type, sysstate.drive_args.drive_cap * 1024 * 1024,
                sysstate.drive_args.rec_speed, 1, sysstate.top_d_shards, sysstate.top_p_shards)
        repair = Repair(sys, sysstate.place_type)
        placement = Placement(sys, sysstate.place_type)

        start = time.time()
        for iter in range(0, iters):
            logging.info("")
            if sysstate.mode == 'RAID':
                res += tick_raid_huan(sysstate, mytimer, sys, repair, placement)
            elif sysstate.mode == 'DP':
                res += tick_dp(sysstate, mytimer, sys, repair, placement)
            elif sysstate.mode == 'MLEC':
                res += tick_mlec_raid(sysstate, mytimer, sys, repair, placement)
        end = time.time()
        print("totaltime: {}".format(end - start))
        return (res, mytimer)
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
    ress = wait_futures(futures)
    
    executor.shutdown()
    for res in ress:
        failed_instances += res[0]
        print(res[1])
        
    return [failed_instances, epochs * iters]

if __name__ == "__main__":
    logger = logging.getLogger()
    # logging.basicConfig(level=logging.INFO)

    for afr in range(5, 6):
        l1args = DriveArgs(d_shards=8, p_shards=2, afr=afr, drive_cap=100, rec_speed=10)
        l1sys = SysState(total_drives=100, drive_args=l1args, placement='MLEC', drives_per_server=10, 
                        top_d_shards=8, top_p_shards=2)

        # res = simulate(l1sys, iters=100000, epochs=24, concur=24)
        res = simulate(l1sys, iters=1000, epochs=1, concur=1)
        break
        res = simulate(l1sys, iters=50000, epochs=80, concur=80)
        while res[0] < 20:
            print(res)
            temp = simulate(l1sys, iters=50000, epochs=80, concur=80)
            res[0] += temp[0]
            res[1] += temp[1]
        # res = simulate(l1sys, iters=1000, epochs=1, concur=1)
        print('++++++++++++++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))

        if res[0] == 0:
            print("NO FAILURE!")
        else:
            # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
            nn = str(round(-math.log10(res[0]/res[1]),3))
            sigma = str(round(1/(math.log(10) * (res[0]**0.5)),3))
            print("Num of Nine: " + nn)
            print("error sigma: " + sigma)

            output = open("s-result-{}.log".format(l1sys.mode), "a")
            output.write("{}-{}-{}-{} {} {} {} {}\n".format(l1args.data_shards, l1args.parity_shards, l1args.afr_in_pct, l1sys.total_drives, nn, sigma, res[0], res[1]))
            output.close()

    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_afr=50&d_cap=20&dr_rw_speed=50&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&spares=0&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&ec_mode=3&d_afr=2&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&tnhost_per_chass=8&tndrv_per_dg=8&ph_nspares=0&tnchassis=1&tnrack=1&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&nhost_per_chass=1&ndrv_per_dg=50&spares=0&woa_nhost_per_chass=1&rec_wr_spd_alloc=100