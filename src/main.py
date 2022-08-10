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
from metrics import Metrics
import time

import argparse


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
    return sim.run_simulation(sysstate, mytimer)

def tick_raid(state_, mytimer: Mytimer, sys, repair, placement):
    # sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, num_disks, num_disks_per_server, 
    #                 k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio)
    np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
    sysstate = state_
    sysstate.gen_drives()
    sim = Simulate(YEAR, sysstate.total_drives, sys, repair, placement)
    return sim.run_simulation(sysstate, mytimer)

def tick_mlec_raid(sysstate, mytimer: Mytimer, sys, repair, placement):
    # sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, num_disks, num_disks_per_server, 
    #                 k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio)
    sim = Simulate(YEAR, sysstate.total_drives, sys, repair, placement)
    return sim.run_simulation(sysstate, mytimer)

def tick_raid_net(state_, mytimer: Mytimer, sys, repair, placement):
    # sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, num_disks, num_disks_per_server, 
    #                 k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio)
    np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
    sysstate = state_
    sysstate.gen_drives()
    sim = Simulate(YEAR, sysstate.total_drives, sys, repair, placement)
    return sim.run_simulation(sysstate, mytimer)

def iter(state_: SysState, iters):
    
    try:
        res = 0
        sysstate = copy.deepcopy(state_)
        mytimer = Mytimer()
        sys = Trinity(sysstate.total_drives, sysstate.drives_per_server, sysstate.drive_args.data_shards,
                sysstate.drive_args.parity_shards, sysstate.place_type, sysstate.drive_args.drive_cap * 1024 * 1024,
                sysstate.drive_args.rec_speed, 1, sysstate.top_d_shards, sysstate.top_p_shards,
                sysstate.adapt, sysstate.server_fail)
        repair = Repair(sys, sysstate.place_type)
        placement = Placement(sys, sysstate.place_type)

        # start = time.time()
        for iter in range(0, iters):
            logging.info("")
            if sysstate.mode == 'RAID':
                res += tick_raid(sysstate, mytimer, sys, repair, placement)
            elif sysstate.mode == 'DP':
                res += tick_dp(sysstate, mytimer, sys, repair, placement)
            elif sysstate.mode == 'MLEC':
                res += tick_mlec_raid(sysstate, mytimer, sys, repair, placement)
            elif sysstate.mode == 'RAID-NET':
                res += tick_raid_net(sysstate, mytimer, sys, repair, placement)

        # end = time.time()
        # print("totaltime: {}".format(end - start))
        return (res, mytimer, sys.metrics)
    except Exception as e:
        print(traceback.format_exc())
        return None

# This is a parallel/multi-iter wrapper around iter(state)
def simulate(state, iters, epochs, concur=10):
    # So tick(state) is for a single system, and we want to simulate multiple systems
    executor = ProcessPoolExecutor(concur)
    
    failed_instances = 0
    futures = []
    metrics = Metrics()

    for epoch in range(0, epochs):
        futures.append(executor.submit(iter, state, iters))
    ress = wait_futures(futures)
    
    executor.shutdown()
    for res in ress:
        failed_instances += res[0]
        metrics += res[2]
        
    print(metrics)
    return [failed_instances, epochs * iters, metrics]


def normal_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net,
                total_drives, drives_per_server, placement):
    # logging.basicConfig(level=logging.INFO)
    for cap in range(20, 30, 10):
        drive_args1 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state1 = SysState(total_drives=total_drives, drive_args=drive_args1, placement=placement, drives_per_server=drives_per_server, 
                        top_d_shards=N_net, top_p_shards=k_net, adapt=adapt, server_fail = 0)


        # res = simulate(sys_state1, iters=100000, epochs=24, concur=24)
        # res = simulate(sys_state1, iters=1000, epochs=1, concur=1)
        # break
        res = [0, 0]
        while res[0] < 20:
            start  = time.time()
            temp = simulate(sys_state1, iters=50000, epochs=200, concur=200)
            res[0] += temp[0]
            res[1] += temp[1]
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print(res)
        # res = simulate(sys_state1, iters=1000, epochs=1, concur=1)
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

            output = open("s-result-{}.log".format(placement), "a")
            output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {}\n".format(
                N_local, k_local, N_net, k_net, total_drives,
                afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt"))
            output.close()


def approx_1_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_server, placement):
    for cap in range(20, 30, 10):
        drive_args1 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state1 = SysState(total_drives=drives_per_server, drive_args=drive_args1, placement='RAID', drives_per_server=drives_per_server, 
                        top_d_shards=N_net, top_p_shards=0, adapt=adapt, server_fail = 0)

        print('')
        # res = simulate(l1sys, iters=100000, epochs=24, concur=24)
        # res = simulate(l1sys, iters=100, epochs=1, concur=1)
        # break
        res = [0, 0]
        while res[0] < 20:
            start  = time.time()
            temp = simulate(sys_state1, iters=50000, epochs=200, concur=200)
            res[0] += temp[0]
            res[1] += temp[1]
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print(res)
        # res = simulate(l1sys, iters=1000, epochs=1, concur=1)
        server_one_fail_prob = res[0] / res[1]
        print('++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that server one fails: {}'.format(server_one_fail_prob))

        pro_sys_fail_contain_s1 = 1 - math.comb(N_net + k_net - 1, k_net + 1) / math.comb(N_net + k_net, k_net + 1)
        print('------------')
        print('Probability that system failure contains server one: {}'.format(pro_sys_fail_contain_s1))


        drive_args2 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state2 = SysState(total_drives=total_drives, drive_args=drive_args2, placement=placement, drives_per_server=drives_per_server, 
                        top_d_shards=N_net, top_p_shards=k_net, adapt=adapt, server_fail = 1)

        res = [0, 0]
        while res[0] < 20:
            start  = time.time()
            temp = simulate(sys_state2, iters=50000, epochs=200, concur=200)
            res[0] += temp[0]
            res[1] += temp[1]
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print(res)
        # res = simulate(l1sys, iters=1000, epochs=1, concur=1)
        conditional_prob = res[0] / res[1]
        print('------------')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that the system fails when server one fails: {}'.format(conditional_prob))

        print('------------')
        res[0] = res[0] * server_one_fail_prob / pro_sys_fail_contain_s1
        aggr_prob = conditional_prob * server_one_fail_prob / pro_sys_fail_contain_s1
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that the system fails: {}'.format(aggr_prob))

        # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
        nn = str(round(-math.log10(res[0]/res[1]),3))
        sigma = str(round(1/(math.log(10) * (res[0]**0.5)),3))
        print("Num of Nine: " + nn)
        print("error sigma: " + sigma)

        output = open("s-result-{}.log".format(placement), "a")
        output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {}\n".format(
                N_local, k_local, N_net, k_net, total_drives,
                afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt"))
        output.close()


def approx_2_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_server, placement):
    for cap in range(100, 110, 10):
        drive_args1 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state1 = SysState(total_drives=drives_per_server, drive_args=drive_args1, placement='RAID', drives_per_server=drives_per_server, 
                        top_d_shards=N_net, top_p_shards=0, adapt=adapt, server_fail = 0)

        print('')
        # res = simulate(l1sys, iters=100000, epochs=24, concur=24)
        # res = simulate(l1sys, iters=100, epochs=1, concur=1)
        # break
        res = [0, 0]
        while res[0] < 20:
            start  = time.time()
            temp = simulate(sys_state1, iters=50000, epochs=200, concur=200)
            res[0] += temp[0]
            res[1] += temp[1]
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print(res)
        # res = simulate(l1sys, iters=1000, epochs=1, concur=1)
        server_one_fail_prob = res[0] / res[1]
        server_one_and_two_fail_prob = server_one_fail_prob ** 2
        print('++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that server one fails: {}'.format(server_one_fail_prob))
        print('Probability that server one and two fails: {}'.format(server_one_and_two_fail_prob))

        pro_sys_fail_contain_s1_s2 = math.comb(N_net + k_net - 2, k_net + 1 - 2) / math.comb(N_net + k_net, k_net + 1)
        print('------------')
        print('Probability that system failure contains server one: {}'.format(pro_sys_fail_contain_s1_s2))


        drive_args2 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state2 = SysState(total_drives=total_drives, drive_args=drive_args2, placement=placement, drives_per_server=drives_per_server, 
                        top_d_shards=N_net, top_p_shards=k_net, adapt=adapt, server_fail = 2)

        res = [0, 0]
        while res[0] < 20:
            start  = time.time()
            temp = simulate(sys_state2, iters=50000, epochs=200, concur=200)
            res[0] += temp[0]
            res[1] += temp[1]
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print(res)
        # res = simulate(l1sys, iters=1000, epochs=1, concur=1)
        conditional_prob = res[0] / res[1]
        print('------------')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that the system fails when server one fails: {}'.format(conditional_prob))

        print('------------')
        res[0] = res[0] * server_one_and_two_fail_prob / pro_sys_fail_contain_s1_s2
        aggr_prob = conditional_prob * server_one_and_two_fail_prob / pro_sys_fail_contain_s1_s2
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that the system fails: {}'.format(aggr_prob))

        # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
        nn = str(round(-math.log10(res[0]/res[1]),3))
        sigma = str(round(1/(math.log(10) * (res[0]**0.5)),3))
        print("Num of Nine: " + nn)
        print("error sigma: " + sigma)

        output = open("s-result-{}.log".format(placement), "a")
        output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {}\n".format(
                N_local, k_local, N_net, k_net, total_drives,
                afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt"))
        output.close()



if __name__ == "__main__":
    logger = logging.getLogger()
    # logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Parse simulator configurations.')
    parser.add_argument('-manualFail', type=int, help="number of manual failed servers.", default=0)
    parser.add_argument('-afr', type=int, help="disk annual failure rate.", default=5)
    parser.add_argument('-io_speed', type=int, help="disk repair rate.", default=30)
    parser.add_argument('-cap', type=int, help="disk capacity (TB)", default=20)
    parser.add_argument('-adapt', type=bool, help="assume seagate adapt or not", default=False)
    parser.add_argument('-n_local', type=int, help="number of data chunks in local EC", default=7)
    parser.add_argument('-k_local', type=int, help="number of parity chunks in local EC", default=1)
    parser.add_argument('-n_net', type=int, help="number of data chunks in network EC", default=7)
    parser.add_argument('-k_net', type=int, help="number of parity chunks in network EC", default=1)
    parser.add_argument('-total_drives', type=int, help="number of total drives in the system", default=-1)
    parser.add_argument('-drives_per_server', type=int, help="number of drives per server", default=-1)
    parser.add_argument('-placement', type=str, help="placement policy. Can be RAID/DP/MLEC/LRC", default='MLEC')
    args = parser.parse_args()

    sim_mode = args.manualFail

    afr = args.afr
    io_speed = args.io_speed
    cap = args.cap
    adapt = args.adapt
    N_local = args.n_local
    k_local = args.k_local
    N_net = args.n_net
    k_net = args.k_net

    total_drives = args.total_drives
    if total_drives == -1:
        total_drives = (N_local+k_local) * (N_net+k_net)


    drives_per_server = args.drives_per_server
    if drives_per_server == -1:
        drives_per_server=N_local+k_local
    
    placement = args.placement
    if placement in ['RAID', 'DP']:
        k_net = 0

    if sim_mode == 0:
        normal_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_server, placement)
    elif sim_mode == 1:
        approx_1_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_server, placement)
    elif sim_mode == 2:
        approx_2_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_server, placement)


    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_afr=50&d_cap=20&dr_rw_speed=50&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&spares=0&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&ec_mode=3&d_afr=2&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&tnhost_per_chass=8&tndrv_per_dg=8&ph_nspares=0&tnchassis=1&tnrack=1&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&nhost_per_chass=1&ndrv_per_dg=50&spares=0&woa_nhost_per_chass=1&rec_wr_spd_alloc=100