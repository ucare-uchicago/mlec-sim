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

from system import System

from simulate import Simulate
from mytimer import Mytimer
from metrics import Metrics

import argparse
import time




def burst_brute(k_net, p_net, k_local, p_local):
    for num_fail_disks in range(2,10):
        n_local = k_local + p_local
        n_net = k_net + p_net
        num_disks = n_local * n_net
        num_racks = n_net
        num_system_status = 2 ** num_disks     # bitmap for drive status. 0 for healthy; 1 for failed. 2^Ndrives possible combinations

        num_affect_racks = 2

        total_cases = 0     # total cases when 4 disks and 2 racks are affected
        data_losses = 0     # data loss cases when 4 disks and 2 racks are affected

        loop1_time = 0
        loop2_time = 0
        end = time.time()
        system_states = np.array(range(num_system_status))
        disks_states = []
        for diskid in range(num_disks):
            A = (2 ** (diskid+1))
            B = (2 ** diskid)
            disk_states = system_states % A // B
            disks_states.append(disk_states)
        disks_states = np.array(disks_states)

        racks_states = []
        affected_racks_count = np.zeros(num_system_status)
        system_failues_count = np.zeros(num_system_status)
        for rackid in range(n_net):
            rack_states = np.zeros(num_system_status)
            for x in range(n_local):
                diskid = rackid * n_local + x
                rack_states += disks_states[diskid]
            system_failues_count += rack_states
            racks_states.append(rack_states)

            rack_affected_or_not = np.sign(rack_states)
            affected_racks_count += rack_affected_or_not
        racks_states = np.array(racks_states)
        
        for i in range(num_system_status):
            if system_failues_count[i] == num_fail_disks:
                affected_racks = affected_racks_count[i]
                if affected_racks == num_affect_racks:
                    total_cases += 1
                    rack_failures = 0
                    for rackid in range(n_net):
                        if racks_states[rackid][i] > p_local:
                            rack_failures += 1
                    if rack_failures > p_net:
                        data_losses += 1
            loop2_time += time.time() - end
            end = time.time()

        dl_prob = data_losses / total_cases
        print('loop1 time: {}  loop2 time: {}'.format(loop1_time, loop2_time))
        output = open("s-burstBrute.log", "a")
        output.write("({}+{})({}+{}) {} {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, num_fail_disks, dl_prob, num_affect_racks, data_losses, total_cases))
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

    burst_brute(k_net, p_net, k_local, p_local)