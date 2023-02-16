import numpy as np
import math
import logging
import random

import time

import argparse
import pandas as pd

from policies import *

from decimal import *

getcontext().prec = 50

def burst_theory(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks, num_chunks_per_disk):
    if placement == 'RAID':
        return burst_theory_raid(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks)
        
    if placement == 'DP':
        return burst_theory_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)
    
    if placement == 'RAID_NET':
        return burst_theory_net_raid(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks)
    
    if placement == 'NET_DP':
        return burst_theory_net_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks, num_chunks_per_disk)
    
    if placement == 'LRC_DP':
        return burst_theory_lrc_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks, num_chunks_per_disk)
    
    if placement == 'MLEC_CP_CP':
        return burst_theory_mlec_cp_cp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks)
    
    if placement == 'MLEC_CP_DP':
        return burst_theory_mlec_cp_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)
    
    if placement == 'MLEC_DP_CP':
        return burst_theory_mlec_dp_cp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks)
    
    if placement == 'MLEC_DP_DP':
        return burst_theory_mlec_dp_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)


##############################
# local-only slec clustered (local RAID)
##############################
def burst_theory_raid(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks):
    num_racks = total_drives // drives_per_rack
    total_cases = total.total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    survival_cases = raid.survival_count_raid_fixed_racks(k_local, p_local, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)

    dl_prob = 1 - survival_cases/total_cases
    # print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    print("\ntotal: \t\t{:.4E} \nsurvival: \t{:.4E} \ndl prob: \t{}\n".format(total_cases, survival_cases, dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, dl_prob))
    return dl_prob


##############################
# network-only slec clustered (network RAID)
##############################
def burst_theory_net_raid(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks):
    num_racks = total_drives // drives_per_rack
    total_cases = total.total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    num_rackgroups = num_racks // (k_net + p_net)
    print("k_net {} p_net {} num_rackgroups {} drives_per_rack {} num_failed_disks {} num_affected_racks {}".format(
                k_net, p_net, num_rackgroups, drives_per_rack, num_failed_disks, num_affected_racks))
    survival_cases = netraid.survival_count(k_net, p_net, num_rackgroups, drives_per_rack, num_failed_disks, num_affected_racks)

    print(netraid.survival_count_dic)
    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    
    # print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    print("\ntotal: \t\t{} \nsurvival: \t{}".format(total_cases, survival_cases))

    dl_prob = 1 - Decimal(survival_cases)/Decimal(total_cases)
    print("dl prob: \t{}\n".format(dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, dl_prob))
    return dl_prob

##############################
# network-only slec declustered (network dp)
##############################
def burst_theory_net_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks, num_chunks_per_disk):
    num_racks = total_drives // drives_per_rack
    n_net = k_net + p_net
    num_failed_chunks = p_net + 1
    total_cases = total.total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    # all the possible cases for distrbuting a random stripe
    total_stripe_cases = total_cases * math.comb(num_racks, n_net) * (drives_per_rack ** n_net)
    print("k_net {} p_net {} drives_per_rack {} num_failed_disks {} num_affected_racks {}".format(
                k_net, p_net, drives_per_rack, num_failed_disks, num_affected_racks))

    # all the possible cases for one random stripe to fail under the burst
    stripe_failure_cases = netdp.stripe_fail_cases_correlated(
                n_net, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)

    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    
    # print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    print("\ntotal: \t\t{} \nstripe_failure_cases: \t{}".format(total_stripe_cases, stripe_failure_cases))

    # the probability for a random stripe to survive the burst
    stripe_fail_prob = Decimal(int(stripe_failure_cases))/Decimal(int(total_stripe_cases))
    stripe_survive_prob = 1 - stripe_fail_prob

    # count the number of stripes in the cluster
    num_stripes = total_drives * num_chunks_per_disk // n_net

    # compute the probability of any stripe failure
    dl_prob = 1 - stripe_survive_prob ** num_stripes
    print("dl prob: \t{}\n".format(dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives, num_chunks_per_disk,
            num_failed_disks, num_affected_racks, dl_prob))
    return dl_prob


##############################
# mlec clustered
##############################
def burst_theory_mlec_cp_cp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks):
    num_racks = total_drives // drives_per_rack
    total_cases = total.total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    survival_cases = mlec_cp_cp.survival_count(k_net, p_net, k_local, p_local, total_drives, drives_per_rack, num_failed_disks, num_affected_racks)

    # print(mlec.survival_count_dic)
    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    
    # print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    print("\ntotal: \t\t{} \nsurvival: \t{}".format(total_cases, survival_cases))

    dl_prob = 1 - survival_cases/total_cases
    print("dl prob: \t{}\n".format(dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, dl_prob))
    return dl_prob


##############################
# Azure lrc declustered
##############################
def burst_theory_lrc_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks, num_chunks_per_disk):
    num_racks = total_drives // drives_per_rack
    n_net = k_net + p_net + k_net // k_local
    num_danger_chunks = p_net + 1 + 1
    total_cases = total.total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    # all the possible cases for distrbuting a random stripe
    total_stripe_cases = total_cases * math.comb(num_racks, n_net) * (drives_per_rack ** n_net)
    print("k_net {} p_net {} drives_per_rack {} num_failed_disks {} num_affected_racks {}".format(
                k_net, p_net, drives_per_rack, num_failed_disks, num_affected_racks))

    # all the possible cases for one random stripe to fail under the burst
    print("n_net: {} num_danger_chunks: {} num_racks {} drives_per_rack {} num_failed_disks{} num_affected_racks {}".format(
            n_net, num_danger_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks
    ))
    stripe_danger_cases = lrcdp.stripe_fixed_fail_chunk_cases_correlated(n_net, num_danger_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    stripe_other_failure_cases = lrcdp.stripe_fail_cases_correlated(n_net, num_danger_chunks+1, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)

    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    
    # print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    print("\n total: \t\t\t\t{} \n stripe_critical_cases: \t{}\n stripe_other_failure_cases: \t{}".format(
                total_stripe_cases, stripe_danger_cases, stripe_other_failure_cases))

    danger_fail_prob = lrcdp.calculate_recoverability(k_net, k_net//k_local, p_net, num_danger_chunks)
    print("danger_fail_prob: {}".format(danger_fail_prob))

    # the probability for a random stripe to survive the burst
    stripe_fail_prob = (Decimal(int(stripe_danger_cases)) / Decimal(int(total_stripe_cases)) * Decimal(danger_fail_prob) 
                            + Decimal(int(stripe_other_failure_cases)) / Decimal(int(total_stripe_cases)))
    stripe_survive_prob = 1 - stripe_fail_prob

    print("stripe_survive_prob: {}".format(stripe_survive_prob))

    # count the number of stripes in the cluster
    num_stripes = total_drives * num_chunks_per_disk // n_net

    # compute the probability of any stripe failure
    dl_prob = 1 - stripe_survive_prob ** num_stripes
    print("dl prob: \t{}\n".format(dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives, num_chunks_per_disk,
            num_failed_disks, num_affected_racks, dl_prob))
    return dl_prob




##############################
# mlec declustered
##############################
def burst_theory_mlec_cp_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks):
    num_racks = total_drives // drives_per_rack
    total_cases = total.total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    survival_cases = mlec_cp_dp.survival_count(k_net, p_net, k_local, p_local, total_drives, drives_per_rack, drives_per_diskgroup, num_failed_disks, num_affected_racks)

    # print(mlec.survival_count_dic)
    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    
    # print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    print("\ntotal: \t\t{} \nsurvival: \t{}".format(total_cases, survival_cases))

    dl_prob = 1 - survival_cases/total_cases
    print("dl prob: \t{}\n".format(dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, dl_prob))
    return dl_prob


##############################
# mlec dp-cp
##############################
def burst_theory_mlec_dp_cp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, placement, num_failed_disks, num_affected_racks):
    num_racks = total_drives // drives_per_rack
    total_cases = total.total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    survival_cases = mlec_dp_cp.survival_count_system(k_net, p_net, k_local, p_local, num_racks, drives_per_rack, 
                                    num_failed_disks, num_affected_racks)

    # print(mlec.survival_count_dic)
    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    
    # print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    print("\ntotal: \t\t{} \nsurvival: \t{}".format(total_cases, survival_cases))

    dl_prob = 1 - survival_cases/total_cases
    print("dl prob: \t{}\n".format(dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, dl_prob))
    return dl_prob

##############################
# mlec dp-dp
##############################
def burst_theory_mlec_dp_dp(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks):
    num_racks = total_drives // drives_per_rack
    total_cases = total.total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    survival_cases = mlec_dp_dp.survival_count_system(k_net, p_net, k_local, p_local, 
                    num_racks, drives_per_rack, drives_per_diskgroup, num_failed_disks, num_affected_racks)

    # print(mlec.survival_count_dic)
    print("num_failed_disks: {} num_affected_racks: {}".format(num_failed_disks, num_affected_racks))
    
    # print("total: {:.4E} survival: {:.4E} dl prob: {}".format(total, survival, dl_prob))
    print("\ntotal: \t\t{} \nsurvival: \t{}".format(total_cases, survival_cases))

    dl_prob = 1 - survival_cases/total_cases
    print("dl prob: \t{}\n".format(dl_prob))
    with open("s-burst-theory-{}.log".format(placement), "a") as output:
        output.write("({}+{})({}+{}) {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            num_failed_disks, num_affected_racks, dl_prob))
    return dl_prob


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
    parser.add_argument('-num_chunks_per_disk', type=int, help="num chunks per disk. Ideally we assume infinite, but in reality it's a finite number ", default=1000)

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

    num_chunks_per_disk = args.num_chunks_per_disk

    # for num_failed_disks in range(4, 50):
    #     # for num_affected_racks in range(1, num_failed_disks+1):
        # for num_affected_racks in range(4,5):

    for num_failed_disks in range(1, 61):
        max_racks = min(40, num_failed_disks)
        for num_affected_racks in range(1,max_racks+1):
            burst_theory(k_net, p_net, k_local, p_local, 
                total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks, num_chunks_per_disk)

    # temp = time.time()
    # for num_failed_disks in range(3, 4):
    #     for num_affected_racks in range(3,4):
    #         burst_theory(k_net, p_net, k_local, p_local, 
    #             total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks)
    # print("time: {}".format(time.time()-temp))





    # df = pd.read_csv ('failures/exp/ornl/by_rack/burst_node_rack.csv')
    # print(df)
    # failure_list = []
    # counts = {}
    # total_count = 0
    # for index, row in df.iterrows():
    #     num_affected_racks = row['racks']
    #     num_failed_disks = row['drives']
    #     count = int(row['count'])
    #     failure_list.append((num_affected_racks, num_failed_disks))
    #     counts[(num_affected_racks, num_failed_disks)] = count
    #     total_count += count
    # # aggr_prob = 0
    # # for i in range(len(failure_list)):
    # #     num_affected_racks, num_failed_disks = failure_list[i]

    # #     prob_dl = burst_theory(k_net, p_net, k_local, p_local, 
    # #                     total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks, num_chunks_per_disk)
    # #     if failure_list[i] in counts:
    # #         aggr_prob += prob_dl * counts[failure_list[i]] / total_count

    # # nines = round(-math.log10(aggr_prob),4)
    # # print("aggr prob data loss: {} nines: {}".format(aggr_prob, nines))
    # aggr_prob = 1
    # for i in range(len(failure_list)):
    #     num_affected_racks, num_failed_disks = failure_list[i]

    #     prob_dl = burst_theory(k_net, p_net, k_local, p_local, 
    #                     total_drives, drives_per_rack, drives_per_diskgroup, placement, num_failed_disks, num_affected_racks, num_chunks_per_disk)
    #     if failure_list[i] in counts:
    #         aggr_prob *= ((1-prob_dl) ** counts[failure_list[i]])

    # nines = round(-math.log10(1-aggr_prob),4)
    # print("aggr prob survival: {} nines: {}".format(aggr_prob, nines))