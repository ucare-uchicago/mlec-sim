import numpy as np
import math
import logging
import random

import time

import argparse
import pandas as pd

def burst_theory(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks):
    if placement == 'RAID':
        burst_theory_raid(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks)
        
    if placement == 'DP':
        burst_theory_dp(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)


# total number of cases for having disk failure bursts in a fixed number of racks
# total_count is a dictionary.
# Key: (num_failed_disks, num_affected_racks)
# Value: total number of cases for f disk failures in r racks
# We compute total_cases_fixed_racks using backtracking, or dynamic programing, whatever you want to call it
# The time complexity is O(f*r)
# The space complexity is O(f*r)
total_count = {}
def total_cases_fixed_racks(drives_per_rack, num_failed_disks, num_affected_racks):
    if (num_failed_disks, num_affected_racks) in total_count:
        return total_count[(num_failed_disks, num_affected_racks)]
    if num_failed_disks < num_affected_racks:
        total_count[(num_failed_disks, num_affected_racks)] = 0
        return 0
    if num_affected_racks == 1:
        if num_failed_disks > drives_per_rack:
            total_count[(num_failed_disks, num_affected_racks)] = 0
        else:
            total_count[(num_failed_disks, num_affected_racks)] = math.comb(drives_per_rack, num_failed_disks)
            return total_count[(num_failed_disks, num_affected_racks)]
    
    
    # we need to make sure every affected rack has at least one disk failure
    # Thus each rack can have at most f-r+1 disk failures.
    max_failures_per_rack = min(num_failed_disks-num_affected_racks+1, drives_per_rack)
    count = 0
    for i in range(1, max_failures_per_rack + 1):
        count += math.comb(drives_per_rack, i) * total_cases_fixed_racks(drives_per_rack, num_failed_disks-i, num_affected_racks-1)
    total_count[(num_failed_disks, num_affected_racks)] = count
    return count


##############################
# local-only slec clustered (local RAID)
##############################
# total number of cases for RAID to survive f disk failures in a single rack. Suppose a rack contains g disk groups.
# survival_count_raid_single_rack is a dictionary.
# Key: (num_failed_disks, num_diskgroups)
# Value: total number of survival cases
# We compute raid_count_single_rack using backtracking, or dynamic programing, whatever you want to call it
# The time complexity is O(f*g)
# The space complexity is O(f*g)
# NOTE: survival_count_raid_single_rack is a global variable, and assumes you have FIXED parity, and disk # per group.
#       If you have multiple different parity numbers, or disk group size, this dictionary will mess up.
#       The best practice is to maintain a nested dictionary:
#       survival_count_raid_single_rack_dic = {(parity, disk_group_size): survival_count_raid_single_rack}
survival_count_raid_single_rack = {}
def raid_count_single_rack(num_failed_disks, num_diskgroups, p_local, disks_per_group):
    if (num_failed_disks, num_diskgroups) in survival_count_raid_single_rack:
        return survival_count_raid_single_rack[(num_failed_disks, num_diskgroups)]
    if num_failed_disks < 0 or num_failed_disks > p_local * num_diskgroups:
         return 0
    if num_diskgroups == 1:
        survival_count_raid_single_rack[(num_failed_disks, num_diskgroups)] = math.comb(disks_per_group, num_failed_disks)
        return survival_count_raid_single_rack[(num_failed_disks, num_diskgroups)]

    max_failures_per_group = min(num_failed_disks, p_local)
    count = 0
    for i in range(max_failures_per_group + 1):
        count += math.comb(disks_per_group, i) * raid_count_single_rack(num_failed_disks-i, num_diskgroups-1, p_local, disks_per_group)
    survival_count_raid_single_rack[(num_failed_disks, num_diskgroups)] = count
    return count
    
# total number of cases for RAID to survive f disk failures in exactly r rack. Each rack should have at least 1 failure
# survival_count_raid_single_rack is a dictionary.
# Key: (num_failed_disks, num_affected_racks)
# Value: total number of survival cases
# We compute survival_count_raid using backtracking, or dynamic programing, whatever you want to call it
# The time complexity is O(f*r)
# The space complexity is O(f*r)
# NOTE: survival_raid_dic is a global variable, and assumes you have FIXED k+p, and drives_per_rack.
#       If you have multiple different parity numbers, or drives_per_rack, this dictionary will mess up.
#       The best practice is to maintain a nested dictionary:
#       e.g. survival_raid_nested_dic = {(k_local, p_local, drives_per_rack): survival_raid_dic}
survival_raid_dic = {}
def survival_count_raid(k_local, p_local, drives_per_rack, num_failed_disks, num_affected_racks):
    if (num_failed_disks, num_affected_racks) in survival_raid_dic:
        return survival_raid_dic[(num_failed_disks, num_affected_racks)]
    if num_failed_disks < num_affected_racks:
        return 0
    disks_per_group = k_local + p_local
    num_diskgroups_per_rack = drives_per_rack // disks_per_group
    if num_affected_racks == 1:
        survival_raid_dic[(num_failed_disks, num_affected_racks)] = raid_count_single_rack(num_failed_disks, num_diskgroups_per_rack, p_local, disks_per_group)
        return survival_raid_dic[(num_failed_disks, num_affected_racks)]
    
    # we need to make sure every affected rack has at least one disk failure
    # Thus each rack can have at most f-r+1 disk failures.
    max_failures_per_rack = min(num_failed_disks-num_affected_racks+1, drives_per_rack)
    count = 0
    for i in range(1, max_failures_per_rack + 1):
        count += (raid_count_single_rack(i, num_diskgroups_per_rack, p_local, disks_per_group) * 
                    survival_count_raid(k_local, p_local, drives_per_rack, num_failed_disks - i, num_affected_racks - 1))
    survival_raid_dic[(num_failed_disks, num_affected_racks)] = count
    return count


def burst_theory_raid(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks):
    total = total_cases_fixed_racks(drives_per_rack, num_failed_disks, num_affected_racks)
    
    survival = survival_count_raid(k_local, p_local, drives_per_rack, num_failed_disks, num_affected_racks)

    dl_prob = 1 - survival/total
    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, dl_prob))




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse simulator configurations.')
    parser.add_argument('-k_local', type=int, help="number of data chunks in local EC", default=7)
    parser.add_argument('-p_local', type=int, help="number of parity chunks in local EC", default=1)
    parser.add_argument('-k_net', type=int, help="number of data chunks in network EC", default=7)
    parser.add_argument('-p_net', type=int, help="number of parity chunks in network EC", default=1)
    parser.add_argument('-total_drives', type=int, help="number of total drives in the system", default=-1)
    parser.add_argument('-drives_per_rack', type=int, help="number of drives per rack", default=-1)
    parser.add_argument('-drives_per_diskgroup', type=int, help="number of drives per disk group", default=-1)
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
    
    drives_per_diskgroup = args.drives_per_diskgroup
    if drives_per_diskgroup == -1:
        drives_per_diskgroup = drives_per_rack
    
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
    # for num_failed_disks in range(4, 101):
    #     # for num_affected_racks in range(1, min(num_failed_disks, 20)+1):
    #     for num_affected_racks in range(4,5):
    temp = time.time()
    for num_failed_disks in range(99, 100):
        for num_affected_racks in range(4,5):
            burst_theory(k_local, p_local, k_net, p_net, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)
    print("time: {}".format(time.time()-temp))
    # temp = time.time()
    # for num_failed_disks in range(100, 101):
    #     for num_affected_racks in range(4,5):
    #         burst_theory(k_local, p_local, k_net, p_net, 
    #             total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)
    # print("time: {}".format(time.time()-temp))