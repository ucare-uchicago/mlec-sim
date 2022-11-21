import numpy as np
import math
import logging
import random

import time

import argparse
import pandas as pd

import netraid, raid

def burst_theory(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks):
    if placement == 'RAID':
        burst_theory_raid(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks)
        
    if placement == 'DP':
        burst_theory_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)
    
    if placement == 'RAID_NET':
        burst_theory_net_raid(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks)
    
    if placement == 'MLEC':
        burst_theory_mlec(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks)


# total number of cases for having disk failure bursts in affecting all racks. Each rack has at least one disk failure
# total_count is a dictionary.
# Key: (num_failed_disks, num_affected_racks)
# Value: total number of cases for f disk failures in r racks
# We compute total_cases_affect_all_racks using backtracking, or dynamic programing, whatever you want to call it
# The time complexity is O(f*r)
# The space complexity is O(f*r)
total_count = {}
def total_cases_affect_all_racks(drives_per_rack, num_failed_disks, num_affected_racks):
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
        count += math.comb(drives_per_rack, i) * total_cases_affect_all_racks(drives_per_rack, num_failed_disks-i, num_affected_racks-1)
    total_count[(num_failed_disks, num_affected_racks)] = count
    return count

# total number of cases for having f disk failure bursts affecting exactly r racks
def total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks):
    return math.comb(num_racks, num_affected_racks) * total_cases_affect_all_racks(drives_per_rack, num_failed_disks, num_affected_racks)


##############################
# local-only slec clustered (local RAID)
##############################
def burst_theory_raid(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks):
    num_racks = total_drives // drives_per_rack
    total = total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    survival = raid.survival_count_raid_fixed_racks(k_local, p_local, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)

    dl_prob = 1 - survival/total
    # print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    print("\ntotal: \t\t{:.4E} \nsurvival: \t{:.4E} \ndl prob: \t{}\n".format(total, survival, dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, dl_prob))


##############################
# network-only slec clustered (network RAID)
##############################
def burst_theory_net_raid(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks):
    num_racks = total_drives // drives_per_rack
    total = total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    num_rackgroups = num_racks // (k_net + p_net)
    print("k_net {} p_net {} num_rackgroups {} drives_per_rack {} num_failed_disks {} num_affected_racks {}".format(
                k_net, p_net, num_rackgroups, drives_per_rack, num_failed_disks, num_affected_racks))
    survival = netraid.survival_count(k_net, p_net, num_rackgroups, drives_per_rack, num_failed_disks, num_affected_racks)

    print(netraid.survival_count_dic)
    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    
    # print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    print("\ntotal: \t\t{} \nsurvival: \t{}".format(total, survival))

    dl_prob = 1 - survival/total
    print("dl prob: \t{}\n".format(dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, dl_prob))


##############################
# mlec clustered
##############################
def burst_theory_mlec(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks):
    num_racks = total_drives // drives_per_rack
    total = total_cases_fixed_racks(drives_per_rack, num_failed_disks, num_affected_racks)
    
    num_rackgroups = num_racks // (k_net + p_net)
    print("k_net {} p_net {} num_rackgroups {} drives_per_rack {} num_failed_disks {} num_affected_racks {}".format(
                k_net, p_net, num_rackgroups, drives_per_rack, num_failed_disks, num_affected_racks))
    survival = netraid.survival_count(k_net, p_net, num_rackgroups, drives_per_rack, num_failed_disks, num_affected_racks)

    print(netraid.survival_count_dic)
    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    
    # print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    print("\ntotal: \t\t{} \nsurvival: \t{}".format(total, survival))

    dl_prob = 1 - survival/total
    print("dl prob: \t{}\n".format(dl_prob))
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
    for num_failed_disks in range(1, 21):
        for num_affected_racks in range(1,num_failed_disks+1):
            burst_theory(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)
    print("time: {}".format(time.time()-temp))
    # temp = time.time()
    # for num_failed_disks in range(100, 101):
    #     for num_affected_racks in range(4,5):
    #         burst_theory(k_net, p_net, k_local, p_local, 
    #             total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)
    # print("time: {}".format(time.time()-temp))