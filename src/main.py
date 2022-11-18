from concurrent.futures import ProcessPoolExecutor

import numpy as np
import math
import copy
import traceback
import logging

# Custom stuff
from failure_generator import FailureGenerator, Weibull, GoogleBurst
from util import wait_futures
from constants.time import YEAR
from constants.PlacementType import parse_placement, PlacementType

from system import System

from simulate import Simulate
from mytimer import Mytimer
from metrics import Metrics

import time
import argparse

from helpers.weibullNines import calculate_weibull_nines


def iter(failureGenerator_: FailureGenerator, sys_, iters, mission):
    try:
        res = 0
        failureGenerator = copy.deepcopy(failureGenerator_)
        sys = copy.deepcopy(sys_)
        mytimer: Mytimer = Mytimer()

        start = time.time()
        for iter in range(0, iters):
            # logging.info("")
            iter_start = time.time()
            temp = time.time()
            sim = Simulate(mission, sys.num_disks, sys)
            mytimer.simInitTime += time.time() - temp
            res += sim.run_simulation(failureGenerator, mytimer)
            iter_end = time.time()
            # print("Finishing iter " + str(iter) + " taking " + str((iter_end - iter_start) * 1000) + "ms")
        end = time.time()
        # print("totaltime: {}".format((end - start) * 1000))
        # print(mytimer)
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
# normal Monte Carlo simulation
# -----------------------------
def normal_sim(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
    # logging.basicConfig(level=logging.INFO, filename="run_"+placement+".log")

    mission = YEAR
    failureGenerator = FailureGenerator(afr)

    place_type = parse_placement(placement)
    
    sys = System(total_drives, drives_per_rack, k_local, p_local, place_type, cap * 1024 * 1024,
            io_speed, intrarack_speed, interrack_speed, 1, k_net, p_net, adapt, rack_fail = 0)

    failed_iters = 0
    total_iters = 0
    metrics = Metrics()

    # temp = simulate(sys_state1, iters=10000, epochs=1, concur=1, mission=mission)
    # return

    # We need to get enough failures in order to compute accurate nines #
    while failed_iters < 20:
        logging.info(">>>>>>>>>>>>>>>>>>> simulation started >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
        start  = time.time()
        res = simulate(failureGenerator, sys, iters=iters, epochs=epoch, concur=concur, mission=mission)
        failed_iters += res[0]
        total_iters += res[1]
        metrics += res[2]
        # print(metrics)
        simulationTime = time.time() - start
        print("simulation time: {}".format(simulationTime))
        print("failed_iters: {}  total_iters: {}".format(failed_iters, total_iters))

    total_iters *= mission/YEAR

    # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
    nines = str(round(-math.log10(failed_iters/total_iters),3))
    sigma = str(round(1/(math.log(10) * (failed_iters**0.5)),3))
    print("Num of Nine: " + nines)
    print("error sigma: " + sigma)
    
    output = open("s-result-{}.log".format(placement), "a")
    output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {}\n".format(
        k_net, p_net, k_local, p_local, total_drives,
        afr, cap, io_speed, nines, sigma, failed_iters, total_iters, "adapt" if adapt else "notadapt"))
    output.close()



# ------------------------
# Manually inject 1 rack failure in each simulation to make it easier to find system failure
# 1. get P(rack 1 fails)
# 2. compute P(rack 1 fails | system fails) using probability theory
# 3. get P(system fails | rack 1 fails) by manually inject 1 rack failure in each simulation
# 4. P(system fails) = P(system fails | rack 1 fails) * P(rack 1 fails) / P(rack 1 fails | system fails)
# ------------------------

def manual_1_rack_failure(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, dist):
    # logging.basicConfig(level=logging.INFO)

    
    for afr in range(5, 6):
        # 1. get P(rack 1 fails)
        place_type = parse_placement(placement)
        local_place_type = PlacementType.RAID        # local RAID
        if place_type == PlacementType.MLEC_DP:         # if MLEC_DP
            local_place_type = PlacementType.DP    # local DP
        failureGenerator = FailureGenerator(afr)
        sys = System(drives_per_rack, drives_per_rack, k_local, p_local, local_place_type, cap * 1024 * 1024,
                io_speed, 1, k_net, p_net, adapt, rack_fail = 0)

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
        num_rack_stripesets = total_drives // drives_per_rack // (k_net + p_net)
        pro_sys_fail_contain_rack_stripeset_1 = (
                    1 - math.comb(num_rack_stripesets - 1, 1) / math.comb(num_rack_stripesets, 1))
        pro_rack_stripeset_1_fail_contain_s1 = (
                    1 - math.comb(k_net + p_net - 1, p_net + 1) / math.comb(k_net + p_net, p_net + 1))
        pro_sys_fail_contain_s1 = pro_sys_fail_contain_rack_stripeset_1 * pro_rack_stripeset_1_fail_contain_s1
        print('------------')
        print('Probability that system failure contains rack one: {}'.format(pro_sys_fail_contain_s1))

        # Compute P(system fails | rack 1 fails)
        failureGenerator2 = FailureGenerator(afr)
        sys2 = System(total_drives, drives_per_rack, k_local, p_local, place_type, cap * 1024 * 1024,
                io_speed, 1, k_net, p_net, adapt, rack_fail = 1)

        res = [0, 0, Metrics()]

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
                k_local, p_local, k_net, p_net, total_drives,
                afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt"))
        output.close()



# ------------------------
# Manually inject 2 rack failure in each simulation to make it easier to find system failure
# 1. get P(rack 1 fails)
# 2. get P(rack 1 and rack 2 fail)
# 3. compute P(rack 1,2 fail | system fails) using probability theory
# 4. get P(system fails | rack 1,2 fails) by manually inject 2 rack failure in each simulation
# 5. P(system fails) = P(system fails | rack 1,2 fail) * P(rack 1,2 fails) / P(rack 1,2 fails | system fails)
# ------------------------
def manual_2_rack_failure(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, dist):
    for cap in range(100, 110, 10):
        # 1. get P(rack 1 fails)
        place_type = parse_placement(placement)
        local_place_type = PlacementType.RAID        # local RAID
        if place_type == PlacementType.MLEC_DP:         # if MLEC_DP
            local_place_type = PlacementType.DP    # local DP
        failureGenerator = FailureGenerator(afr)
        sys = System(drives_per_rack, drives_per_rack, k_local, p_local, local_place_type, cap * 1024 * 1024,
                io_speed, 1, k_net, p_net, adapt, rack_fail = 0)

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
        rack_one_fail_prob = res[0] / res[1]

        # 2. get P(rack 1 and rack 2 fail)
        rack_one_and_two_fail_prob = rack_one_fail_prob ** 2
        print('++++++++++++++++++++')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that rack one fails: {}'.format(rack_one_fail_prob))
        print('Probability that rack one and two fails: {}'.format(rack_one_and_two_fail_prob))

        # 3. compute P(rack 1,2 fail | system fails) using probability theory
        pro_sys_fail_contain_s1_s2 = math.comb(k_net + p_net - 2, p_net + 1 - 2) / math.comb(k_net + p_net, p_net + 1)
        print('------------')
        print('Probability that system failure contains rack one: {}'.format(pro_sys_fail_contain_s1_s2))


        # 4. get P(system fails | rack 1,2 fails) by manually inject 2 rack failure in each simulation
        failureGenerator2 = FailureGenerator(afr)
        sys2 = System(total_drives, drives_per_rack, k_local, p_local, place_type, cap * 1024 * 1024,
                io_speed, 1, k_net, p_net, adapt, rack_fail = 2)

        res = [0, 0, Metrics()]

        while res[0] < 20:
            start  = time.time()
            temp = simulate(failureGenerator2, sys2, iters=50000, epochs=200, concur=200)
            res[0] += temp[0]
            res[1] += temp[1]
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print(res)
        conditional_prob = res[0] / res[1]
        print('------------')
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that the system fails when rack one fails: {}'.format(conditional_prob))

        print('------------')

        # 5. P(system fails) = P(system fails | rack 1,2 fail) * P(rack 1,2 fails) / P(rack 1,2 fails | system fails)
        res[0] = res[0] * rack_one_and_two_fail_prob / pro_sys_fail_contain_s1_s2
        aggr_prob = conditional_prob * rack_one_and_two_fail_prob / pro_sys_fail_contain_s1_s2
        print('Total Fails: ' + str(res[0]))
        print('Total Iters: ' + str(res[1]))
        print('Probability that the system fails: {}'.format(aggr_prob))

        nn = str(round(-math.log10(res[0]/res[1]),3))
        sigma = str(round(1/(math.log(10) * (res[0]**0.5)),3))
        print("Num of Nine: " + nn)
        print("error sigma: " + sigma)

        output = open("s-result-{}.log".format(placement), "a")
        output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {}\n".format(
                k_local, p_local, k_net, p_net, total_drives,
                afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt"))
        output.close()



# ------------------------
# Ignore this function for now
# ------------------------

def io_over_year(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net,
                total_drives, drives_per_rack, placement, distribution):
    pass
    # # logging.basicConfig(level=logging.INFO)
    # rebuildio_prev_year = 0
    # place_type = get_placement_index(placement)

    # for years in range(1,51,1):
    #     mission = years*YEAR
    #     drive_args1 = DriveArgs(d_shards=k_local, p_shards=p_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
    #     sys_state1 = FailureGenerator(total_drives=total_drives, drive_args=drive_args1, placement=placement, drives_per_rack=drives_per_rack, 
    #                     top_d_shards=k_net, top_p_shards=p_net, adapt=adapt, rack_fail = 0, distribution = distribution)

    #     res = [0, 0, Metrics()]
    #     start  = time.time()
    #     temp = simulate(sys_state1, iters=int(10000000/200/years), epochs=200, concur=200, mission=mission)
    #     res[0] += temp[0]
    #     res[1] += temp[1]
    #     res[2] += temp[2]
    #     print(res[2])
    #     simulationTime = time.time() - start
    #     print("simulation time: {}".format(simulationTime))
    #     # res = simulate(sys_state1, iters=1000, epochs=1, concur=1)
    #     print('++++++++++++++++++++++++++++++++')
    #     print('Total Fails: ' + str(res[0]))
    #     print('Total Iters: ' + str(res[1]))

    #     res[1] *= years

    #     rebuildio = res[2].getAverageRebuildIO() - rebuildio_prev_year
    #     rebuildio_prev_year = res[2].getAverageRebuildIO()

    #     if res[0] == 0:
    #         print("NO FAILURE!")
    #         nn = 'N/A'
    #         sigma = 'N/A'
    #     else:
    #         # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
    #         nn = str(round(-math.log10(res[0]/res[1]),3))
    #         sigma = str(round(1/(math.log(10) * (res[0]**0.5)),3))
    #         print("Num of Nine: " + nn)
    #         print("error sigma: " + sigma)

    #     output = open("s-rebuildio-{}.log".format(placement), "a")
    #     output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {} {} {}\n".format(
    #         k_local, p_local, k_net, p_net, total_drives,
    #         afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt",
    #         years, rebuildio))
    #     output.close()


def weibull_sim(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net,
                total_drives, drives_per_rack, placement, distribution):
    # logging.basicConfig(level=logging.INFO)
    place_type = parse_placement(placement)
    
    for beta in np.arange(1, 2.2, 0.2):
        mission = YEAR
        t_l = 5  # 5 years
        alpha = t_l / ((- t_l * math.log((1-afr/100))) ** (1/beta))  # in years

        print("{} {} {} {} {} {} {}".format(afr/100,beta,cap,io_speed,drives_per_rack,k_local,p_local))
        nines_from_calculator = calculate_weibull_nines(afr=afr/100, beta=beta, 
                                                        disk_cap=cap, io=io_speed,
                                                        n=drives_per_rack, k=k_local, c=p_local)
        rack_num = total_drives / drives_per_rack
        nines_from_calculator -= math.log10(rack_num)
        print("nines_from_calculator: {}".format(nines_from_calculator))

        distribution = Weibull(alpha, beta)
        print("alpha: {}  beta: {}".format(alpha, beta))

        failureGenerator = FailureGenerator(afr, distribution)
        
        sys = System(total_drives, drives_per_rack, k_local, p_local, place_type, cap * 1024 * 1024,
                io_speed, 1, k_net, p_net, adapt, rack_fail = 0)

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

            output = open("s-weibull-{}.log".format(placement), "a")
            output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {} {} {} {}\n".format(
                k_local, p_local, k_net, p_net, total_drives,
                afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt",
                alpha, beta, nines_from_calculator))
            output.close()



# --------------------------------
# Get metrics
# No need to get enough failures
# Just run enough simulations so that we get the average metrics
# --------------------------------
def metric_sim(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net,
                total_drives, drives_per_rack, placement, distribution):
    place_type = parse_placement(placement)

    for afr in range(2, 6):
        mission = YEAR
        failureGenerator = FailureGenerator(afr)
        
        sys = System(total_drives, drives_per_rack, k_local, p_local, place_type, cap * 1024 * 1024,
                io_speed, 1, k_net, p_net, adapt, rack_fail = 0)

        res = [0, 0, Metrics()]

        start  = time.time()
        temp = temp = simulate(failureGenerator, sys, iters=50000, epochs=200, concur=200, mission=mission)
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
            k_local, p_local, k_net, p_net, total_drives,
            afr, cap, io_speed, res[0], res[1], "adapt" if adapt else "notadapt",
            res[2].getAverageRebuildIO(), res[2].getAverageNetTraffic(), res[2].getAvgNetRepairTime()))
        output.close()


# -----------------------------
# simulate against bursts
# -----------------------------
def burst_sim(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net,
                total_drives, drives_per_rack, placement, distribution):
    # logging.basicConfig(level=logging.INFO)
    
    mission = YEAR
    failureGenerator = FailureGenerator(afr, GoogleBurst(50, 50), is_burst=True)

    place_type = parse_placement(placement)
    
    sys = System(total_drives, drives_per_rack, k_local, p_local, place_type, cap * 1024 * 1024,
            io_speed, 1, k_net, p_net, adapt, rack_fail = 0)

    failed_iters = 0
    total_iters = 0
    metrics = Metrics()

    # res = simulate(failureGenerator, sys, iters=100, epochs=1, concur=1, mission=mission)
    # return

    # We need to get enough failures in order to compute accurate nines #
    while failed_iters < 20:
        start  = time.time()
        res = simulate(failureGenerator, sys, iters=5000, epochs=200, concur=200, mission=mission)
        failed_iters += res[0]
        total_iters += res[1]
        metrics += res[2]
        # print(metrics)
        simulationTime = time.time() - start
        print("simulation time: {}".format(simulationTime))
        print("failed_iters: {}  failed_iters: {}".format(failed_iters, total_iters))

    total_iters *= mission/YEAR

    # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
    prob_dl = str(failed_iters/total_iters)
    nines = str(round(-math.log10(failed_iters/total_iters),3))
    sigma = str(round(1/(math.log(10) * (failed_iters**0.5)),3))
    print("probability of data loss: " + prob_dl)
    print("Num of Nine: " + nines)
    print("error sigma: " + sigma)
    print()

    output = open("s-result-{}.log".format(placement), "a")
    output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {}\n".format(
        k_net, p_net, k_local, p_local, total_drives,
        cap, io_speed, prob_dl, nines, sigma, failed_iters, total_iters, "adapt" if adapt else "notadapt"))
    output.close()



if __name__ == "__main__":
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(description='Parse simulator configurations.')
    parser.add_argument('-sim_mode', type=int, help="simulation mode. Default is 0", default=0)
    parser.add_argument('-afr', type=int, help="disk annual failure rate.", default=5)
    parser.add_argument('-io_speed', type=int, help="disk repair rate (MB/s).", default=30)
    parser.add_argument('-intrarack_speed', type=int, help="Intra-rack speed (GB/s).", default=100)
    parser.add_argument('-interrack_speed', type=int, help="Inter-rack speed (GB/s).", default=10)
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
    parser.add_argument('-concur', type=int, help="how many threads to use concurrently", default=200)
    parser.add_argument('-epoch', type=int, help="how many epochs to run", default=20)
    parser.add_argument('-iter', type=int, help="how many iterations in a epoch in a thread to run", default=50000)
    args = parser.parse_args()

    sim_mode = args.sim_mode

    afr = args.afr
    io_speed = args.io_speed
    cap = args.cap
    adapt = args.adapt
    k_local = args.k_local
    p_local = args.p_local
    k_net = args.k_net
    p_net = args.p_net
    
    intrarack_speed = args.intrarack_speed
    interrack_speed = args.interrack_speed
    
    # Multi-threading stuff
    concur = args.concur
    epoch = args.epoch
    iters = args.iter

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

    if sim_mode == 0:
        normal_sim(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, dist, concur, epoch, iters)
    elif sim_mode == 1:
        manual_1_rack_failure(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, dist)
    elif sim_mode == 2:
        manual_2_rack_failure(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, dist)
    elif sim_mode == 3:
        io_over_year(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, dist)
    elif sim_mode == 4:
        weibull_sim(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, dist)
    elif sim_mode == 5:
        metric_sim(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, dist)
    elif sim_mode == 6:
        burst_sim(afr, io_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, dist)


    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_afr=50&d_cap=20&dr_rw_speed=50&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&spares=0&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&ec_mode=3&d_afr=2&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&tnhost_per_chass=8&tndrv_per_dg=8&ph_nspares=0&tnchassis=1&tnrack=1&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&nhost_per_chass=1&ndrv_per_dg=50&spares=0&woa_nhost_per_chass=1&rec_wr_spd_alloc=100