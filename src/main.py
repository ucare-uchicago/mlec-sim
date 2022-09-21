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
from failure_generator import FailureGenerator, Weibull
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

from helpers.weibullNines import calculate_weibull_nines


def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)


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
            # logging.info("")
            temp = time.time()
            sim = Simulate(mission, sys.num_disks, sys, repair, placement)
            mytimer.simInitTime += time.time() - temp
            res += sim.run_simulation(failureGenerator, mytimer)
        end = time.time()
        # print("totaltime: {}".format(end - start))
        # print(mytimer)
        return (res, mytimer, sys.metrics)
    except Exception as e:
        print(traceback.format_exc())
        return None

# This is a parallel/multi-iter wrapper around iter(state)
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
# normal Monte Carlo simulation
# -----------------------------
def normal_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net,
                total_drives, drives_per_rack, place_type, distribution):
        # logging.basicConfig(level=logging.INFO)
    
    # for afr in range(2, 11):
        mission = YEAR
        failureGenerator = FailureGenerator(afr)
        
        sys = System(total_drives, drives_per_rack, N_local, k_local, place_type, cap * 1024 * 1024,
                io_speed, 1, N_net, k_net, adapt, rack_fail = 0)

        res = [0, 0, Metrics()]

        # temp = simulate(sys_state1, iters=10000, epochs=1, concur=1, mission=mission)
        # return
        while res[0] < 20:
            start  = time.time()
            temp = simulate(failureGenerator, sys, iters=50000, epochs=200, concur=200, mission=mission)
            res[0] += temp[0]
            res[1] += temp[1]
            res[2] += temp[2]
            print(res[2])
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print(res)
        
        # res = simulate(sys_state1, iters=1000, epochs=1, concur=1)
        print('++++++++++++++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))

        res[1] *= mission/YEAR
        

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



# ------------------------
# Manually inject 1 rack failure in each simulation to make it easier to find system failure
# 1. get P(rack 1 fails)
# 2. compute P(rack 1 fails | system fails) using probability theory
# 3. get P(system fails | rack 1 fails) by manually inject 1 rack failure in each simulation
# 4. P(system fails) = P(system fails | rack 1 fails) * P(rack 1 fails) / P(rack 1 fails | system fails)
# ------------------------

def manual_1_rack_failure(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_rack, place_type, dist):
    # logging.basicConfig(level=logging.INFO)

    
    for afr in range(5, 6):
        # 1. get P(rack 1 fails)
        local_place_type = 0        # local RAID
        if place_type == 4:         # if MLEC_DP
            local_place_type = 1    # local DP
        failureGenerator = FailureGenerator(afr)
        sys = System(total_drives, drives_per_rack, N_local, k_local, local_place_type, cap * 1024 * 1024,
                io_speed, 1, N_net, k_net, adapt, rack_fail = 0)

        res = [0, 0, Metrics()]

        # loop until we get enough failures
        while res[0] < 20:
            start  = time.time()
            temp = simulate(failureGenerator, sys, iters=50000, epochs=200, concur=200)
            res[0] += temp[0]
            res[1] += temp[1]
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print(res)
        # res = simulate(l1sys, iters=1000, epochs=1, concur=1)
        rack_one_fail_prob = res[0] / res[1]
        print('++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that rack one fails: {}'.format(rack_one_fail_prob))

        # Compute P(rack 1 fails | system fails)
        #       = P(rack 1 fails | rack_stripeset 1 fails) * P(rack_stripeset 1 fails | system fails)
        num_rack_stripesets = total_drives // drives_per_rack // (N_net + k_net)
        pro_sys_fail_contain_rack_stripeset_1 = (
                    1 - math.comb(num_rack_stripesets - 1, 1) / math.comb(num_rack_stripesets, 1))
        pro_rack_stripeset_1_fail_contain_s1 = (
                    1 - math.comb(N_net + k_net - 1, k_net + 1) / math.comb(N_net + k_net, k_net + 1))
        pro_sys_fail_contain_s1 = pro_sys_fail_contain_rack_stripeset_1 * pro_rack_stripeset_1_fail_contain_s1
        print('------------')
        print('Probability that system failure contains rack one: {}'.format(pro_sys_fail_contain_s1))

        # Compute P(system fails | rack 1 fails)
        failureGenerator2 = FailureGenerator(afr)
        sys2 = System(total_drives, drives_per_rack, N_local, k_local, place_type, cap * 1024 * 1024,
                io_speed, 1, N_net, k_net, adapt, rack_fail = 1)

        res = [0, 0, Metrics()]

        # temp = simulate(sys_state1, iters=10000, epochs=1, concur=1, mission=mission)
        # return
        while res[0] < 20:
            start  = time.time()
            temp = simulate(failureGenerator2, sys2, iters=50000, epochs=200, concur=200)
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
        print('Probability that the system fails when rack one fails: {}'.format(conditional_prob))

        print('------------')

        # P(system fails) = P(system fails | rack 1 fails) * P(rack 1 fails) / P(rack 1 fails | system fails)
        res[0] = res[0] * rack_one_fail_prob / pro_sys_fail_contain_s1
        aggr_prob = conditional_prob * rack_one_fail_prob / pro_sys_fail_contain_s1
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


def manual_2_rack_failure(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_rack, place_type, dist):
    for cap in range(100, 110, 10):
        drive_args1 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state1 = FailureGenerator(total_drives=drives_per_rack, drive_args=drive_args1, placement='RAID', drives_per_rack=drives_per_rack, 
                        top_d_shards=N_net, top_p_shards=0, adapt=adapt, rack_fail = 0)

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
        rack_one_fail_prob = res[0] / res[1]
        rack_one_and_two_fail_prob = rack_one_fail_prob ** 2
        print('++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that rack one fails: {}'.format(rack_one_fail_prob))
        print('Probability that rack one and two fails: {}'.format(rack_one_and_two_fail_prob))

        pro_sys_fail_contain_s1_s2 = math.comb(N_net + k_net - 2, k_net + 1 - 2) / math.comb(N_net + k_net, k_net + 1)
        print('------------')
        print('Probability that system failure contains rack one: {}'.format(pro_sys_fail_contain_s1_s2))


        drive_args2 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state2 = FailureGenerator(total_drives=total_drives, drive_args=drive_args2, placement=placement, drives_per_rack=drives_per_rack, 
                        top_d_shards=N_net, top_p_shards=k_net, adapt=adapt, rack_fail = 2)

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
        print('Probability that the system fails when rack one fails: {}'.format(conditional_prob))

        print('------------')
        res[0] = res[0] * rack_one_and_two_fail_prob / pro_sys_fail_contain_s1_s2
        aggr_prob = conditional_prob * rack_one_and_two_fail_prob / pro_sys_fail_contain_s1_s2
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


def io_over_year(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net,
                total_drives, drives_per_rack, place_type, distribution):
    # logging.basicConfig(level=logging.INFO)
    rebuildio_prev_year = 0

    for years in range(1,51,1):
        mission = years*YEAR
        drive_args1 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state1 = FailureGenerator(total_drives=total_drives, drive_args=drive_args1, placement=placement, drives_per_rack=drives_per_rack, 
                        top_d_shards=N_net, top_p_shards=k_net, adapt=adapt, rack_fail = 0, distribution = distribution)

        res = [0, 0, Metrics()]
        start  = time.time()
        temp = simulate(sys_state1, iters=int(10000000/200/years), epochs=200, concur=200, mission=mission)
        res[0] += temp[0]
        res[1] += temp[1]
        res[2] += temp[2]
        print(res[2])
        simulationTime = time.time() - start
        print("simulation time: {}".format(simulationTime))
        # res = simulate(sys_state1, iters=1000, epochs=1, concur=1)
        print('++++++++++++++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))

        res[1] *= years

        rebuildio = res[2].getAverageRebuildIO() - rebuildio_prev_year
        rebuildio_prev_year = res[2].getAverageRebuildIO()

        if res[0] == 0:
            print("NO FAILURE!")
            nn = 'N/A'
            sigma = 'N/A'
        else:
            # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
            nn = str(round(-math.log10(res[0]/res[1]),3))
            sigma = str(round(1/(math.log(10) * (res[0]**0.5)),3))
            print("Num of Nine: " + nn)
            print("error sigma: " + sigma)

        output = open("s-rebuildio-{}.log".format(placement), "a")
        output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {} {} {}\n".format(
            N_local, k_local, N_net, k_net, total_drives,
            afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt",
            years, rebuildio))
        output.close()



def weibull_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net,
                total_drives, drives_per_rack, place_type, distribution):
    # logging.basicConfig(level=logging.INFO)
    
    for beta in np.arange(1, 2.2, 0.2):
        mission = YEAR
        t_l = 5  # 5 years
        alpha = t_l / ((- t_l * math.log((1-afr/100))) ** (1/beta))  # in years

        print("{} {} {} {} {} {} {}".format(afr/100,beta,cap,io_speed,drives_per_rack,N_local,k_local))
        nines_from_calculator = calculate_weibull_nines(afr=afr/100, beta=beta, 
                                                        disk_cap=cap, io=io_speed,
                                                        n=drives_per_rack, k=N_local, c=k_local)
        rack_num = total_drives / drives_per_rack
        nines_from_calculator -= math.log10(rack_num)
        print("nines_from_calculator: {}".format(nines_from_calculator))

        failure_generator = Weibull(alpha, beta)
        print("alpha: {}  beta: {}".format(alpha, beta))
        drive_args1 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state1 = FailureGenerator(total_drives=total_drives, drive_args=drive_args1, placement=placement, drives_per_rack=drives_per_rack, 
                        top_d_shards=N_net, top_p_shards=k_net, adapt=adapt, rack_fail = 0, failure_generator = failure_generator)

        res = [0, 0, Metrics()]
        while res[0] < 20:
            start  = time.time()
            temp = simulate(sys_state1, iters=50000, epochs=200, concur=200, mission=mission)
            res[0] += temp[0]
            res[1] += temp[1]
            res[2] += temp[2]
            print(res[2])
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print(res)
        # res = simulate(sys_state1, iters=1000, epochs=1, concur=1)
        print('++++++++++++++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))

        res[1] *= mission/YEAR
        

        if res[0] == 0:
            print("NO FAILURE!")
        else:
            # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
            nn = str(round(-math.log10(res[0]/res[1]),3))
            sigma = str(round(1/(math.log(10) * (res[0]**0.5)),3))
            print("Num of Nine: " + nn)
            print("error sigma: " + sigma)

            output = open("s-weibull-{}.log".format(placement), "a")
            output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {} {} {} {}\n".format(
                N_local, k_local, N_net, k_net, total_drives,
                afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt",
                alpha, beta, nines_from_calculator))
            output.close()

def metric_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net,
                total_drives, drives_per_rack, place_type, distribution):
        # logging.basicConfig(level=logging.INFO)
    
    for afr in range(2, 6):
        mission = YEAR
        drive_args1 = DriveArgs(d_shards=N_local, p_shards=k_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        sys_state1 = FailureGenerator(total_drives=total_drives, drive_args=drive_args1, placement=placement, drives_per_rack=drives_per_rack, 
                        top_d_shards=N_net, top_p_shards=k_net, adapt=adapt, rack_fail = 0)

        res = [0, 0, Metrics()]

        # temp = simulate(sys_state1, iters=10000, epochs=1, concur=1, mission=mission)
        # return
        start  = time.time()
        temp = simulate(sys_state1, iters=5000, epochs=200, concur=200, mission=mission)
        res[0] += temp[0]
        res[1] += temp[1]
        res[2] += temp[2]
        print(res[2])

        simulationTime = time.time() - start
        print("simulation time: {}".format(simulationTime))
        print(res)
        
        # res = simulate(sys_state1, iters=1000, epochs=1, concur=1)
        print('++++++++++++++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))

        res[1] *= mission/YEAR
        

        if res[0] == 0:
            print("NO FAILURE!")
            # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))

        output = open("s-metric-{}.log".format(placement), "a")
        output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {} {}\n".format(
            N_local, k_local, N_net, k_net, total_drives,
            afr, cap, io_speed, res[0], res[1], "adapt" if adapt else "notadapt",
            res[2].getAverageRebuildIO(), res[2].getAverageNetTraffic(), res[2].getAvgNetRepairTime()))
        output.close()


if __name__ == "__main__":
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(description='Parse simulator configurations.')
    parser.add_argument('-sim_mode', type=int, help="simulation mode", default=0)
    parser.add_argument('-afr', type=int, help="disk annual failure rate.", default=5)
    parser.add_argument('-io_speed', type=int, help="disk repair rate.", default=30)
    parser.add_argument('-cap', type=int, help="disk capacity (TB)", default=20)
    parser.add_argument('-adapt', type=bool, help="assume seagate adapt or not", default=False)
    parser.add_argument('-n_local', type=int, help="number of data chunks in local EC", default=7)
    parser.add_argument('-k_local', type=int, help="number of parity chunks in local EC", default=1)
    parser.add_argument('-n_net', type=int, help="number of data chunks in network EC", default=7)
    parser.add_argument('-k_net', type=int, help="number of parity chunks in network EC", default=1)
    parser.add_argument('-total_drives', type=int, help="number of total drives in the system", default=-1)
    parser.add_argument('-drives_per_rack', type=int, help="number of drives per rack", default=-1)
    parser.add_argument('-placement', type=str, help="placement policy. Can be RAID/DP/MLEC/LRC", default='MLEC')
    parser.add_argument('-dist', type=str, help="disk failure distribution. Can be exp/weibull", default='exp')
    args = parser.parse_args()

    sim_mode = args.sim_mode

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


    drives_per_rack = args.drives_per_rack
    if drives_per_rack == -1:
        drives_per_rack=N_local+k_local
    
    placement = args.placement
    if placement in ['RAID', 'DP']:
        N_net = 1
        k_net = 0
        
    
    if placement in ['RAID_NET']:
        N_local = 1
        k_local = 0

    dist = args.dist

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

    if sim_mode == 0:
        normal_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_rack, place_type, dist)
    elif sim_mode == 1:
        manual_1_rack_failure(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_rack, place_type, dist)
    elif sim_mode == 2:
        manual_2_rack_failure(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_rack, place_type, dist)
    elif sim_mode == 3:
        io_over_year(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_rack, place_type, dist)
    elif sim_mode == 4:
        weibull_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_rack, place_type, dist)
    elif sim_mode == 5:
        metric_sim(afr, io_speed, cap, adapt, N_local, k_local, N_net, k_net, 
                    total_drives, drives_per_rack, place_type, dist)


    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_afr=50&d_cap=20&dr_rw_speed=50&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&spares=0&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&ec_mode=3&d_afr=2&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&tnhost_per_chass=8&tndrv_per_dg=8&ph_nspares=0&tnchassis=1&tnrack=1&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&nhost_per_chass=1&ndrv_per_dg=50&spares=0&woa_nhost_per_chass=1&rec_wr_spd_alloc=100