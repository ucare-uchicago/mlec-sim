import numpy as np
import math
import logging
import random

import time

import argparse
import pandas as pd



# find all possible fail_disks_per_rack using backtracking algorithm
def find_all_lists(num_failed_disks, num_affected_racks, disks_per_rack):
    # print("num_failed_disks: {} num_affected_racks{} disks_per_rack{}".format(num_failed_disks, num_affected_racks, disks_per_rack))
    if num_affected_racks == 0:
        return []
    if num_affected_racks == 1:
        if num_failed_disks > disks_per_rack:
            return []
        else:
            return [[num_failed_disks]]
    # we need to make sure every rack has at least 1 disk failure
    # therefore, each rack can have up to (num_failed_disks - num_affected_racks + 1) disk failures
    # also, each rack can have at most disks_per_rack disk failures
    res = []
    for i in range(1, min(disks_per_rack, num_failed_disks - num_affected_racks + 1) + 1):
        temp_lists = find_all_lists(num_failed_disks - i, num_affected_racks - 1, disks_per_rack)
        for temp_list in temp_lists:
            temp_list.append(i)
            res.append(temp_list)
    return res


def burst_theory(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement):
    if placement == 'RAID':
            burst_theory_raid(k_local, p_local, k_net, p_net, 
                    total_drives, drives_per_rack, placement, 4, 2)


def total_cases(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement, all_possible_fail_disks_per_rack):
    total_cases_fixed_racks = 0
    for failure_list in all_possible_fail_disks_per_rack:
        cases = 1
        for failures_per_rack in failure_list:
            cases *= math.comb(drives_per_rack, failures_per_rack)
        total_cases_fixed_racks += cases
    return total_cases_fixed_racks



raid_helper = {}
def survival_case_raid(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement, all_possible_fail_disks_per_rack):
    survival_cases = 0
    for failure_list in all_possible_fail_disks_per_rack:
        cases = 1
        for failures_per_rack in failure_list:
            num_diskgroups_per_rack = drives_per_rack // (k_local + p_local)
            # assume failures_per_rack = M > 1
            # suppose a rack has d disk groups
            # here we have x1+x2+x3+...+xd = M
            # for every x we want 0<=x<=p_local
            # how many possible divisions?
            # for every possible division, how many possible combinations? It should be C(n,x1)*C(n,x2)*...C(n,x)
            # Here we resolve this problem using dynamic programming
            def helper(k_local, p_local, failures_per_rack, num_diskgroups_per_rack):
                if p_local == 0:
                    return 0
                if failures_per_rack < 0:
                    return 0
                if failures_per_rack == 0:
                    return 1
                if (k_local, p_local, failures_per_rack, num_diskgroups_per_rack) in raid_helper:
                    return raid_helper[(k_local, p_local, failures_per_rack, num_diskgroups_per_rack)]

                num_disks_per_rack = (k_local + p_local) * num_diskgroups_per_rack
                n_local = k_local + p_local
                # only one disk group. Easy as we simply check if failure # larger than parity
                if num_diskgroups_per_rack == 1:
                    if failures_per_rack <= p_local:
                        raid_helper[(k_local, p_local, failures_per_rack, num_diskgroups_per_rack)] = math.comb(num_disks_per_rack, failures_per_rack)
                    else:
                        raid_helper[(k_local, p_local, failures_per_rack, num_diskgroups_per_rack)] = 0
                    return raid_helper[(k_local, p_local, failures_per_rack, num_diskgroups_per_rack)]
                # only one failure. Then just a random disk in the rack
                if failures_per_rack == 1:
                    raid_helper[(k_local, p_local, failures_per_rack, num_diskgroups_per_rack)] = num_disks_per_rack
                    return num_disks_per_rack
                count = 0
                # dynamic programming. Suppose one disk group contains i failures:
                for i in range(p_local+1):
                    if i > failures_per_rack:
                        break
                    count += helper(k_local, p_local, failures_per_rack - i, num_diskgroups_per_rack - 1) * math.comb(n_local, i)
                raid_helper[(k_local, p_local, failures_per_rack, num_diskgroups_per_rack)] = count
                return count
            
            cases *= helper(k_local, p_local, failures_per_rack, num_diskgroups_per_rack)
            # print("failures_per_rack: {} num_diskgroups_per_rack: {} . helper: {}".format(
            #             failures_per_rack, num_diskgroups_per_rack, helper(k_local, p_local, failures_per_rack, num_diskgroups_per_rack)))
        # print("failure_list: {}, raid cases: {}".format(failure_list, cases))
        survival_cases += cases
    return survival_cases





def burst_theory_raid(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks):
    all_possible_fail_disks_per_rack = find_all_lists(num_failed_disks, num_affected_racks, drives_per_rack)
    total = total_cases(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement, all_possible_fail_disks_per_rack)
    survival = survival_case_raid(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement, all_possible_fail_disks_per_rack)

    print("total: {} survival: {} dl prob: {}".format(total, survival, 1 - survival/total))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, survival/total,))
    




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse simulator configurations.')
    parser.add_argument('-k_local', type=int, help="number of data chunks in local EC", default=7)
    parser.add_argument('-p_local', type=int, help="number of parity chunks in local EC", default=1)
    parser.add_argument('-k_net', type=int, help="number of data chunks in network EC", default=7)
    parser.add_argument('-p_net', type=int, help="number of parity chunks in network EC", default=1)
    parser.add_argument('-total_drives', type=int, help="number of total drives in the system", default=-1)
    parser.add_argument('-drives_per_rack', type=int, help="number of drives per rack", default=-1)
    parser.add_argument('-placement', type=str, help="placement policy. Can be RAID/DP/MLEC/LRC", default='MLEC')
    args = parser.parse_args()

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

    print("(erasure:{}+{})/({}+{})\ntotal drives:\t{}\ndrives_per_rack:\t{}\nplacement:\t{}".format(
                k_net, p_net, k_local, p_local, total_drives, drives_per_rack, placement
    ))
    burst_theory(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement)