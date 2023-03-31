import numpy as np
import logging
import math


# Meng: compute priority percent using counting.
def compute_priority_percent(state, affected_racks, curr_rackId, priority):
    if len(affected_racks) == 1 and priority == 1:
        return 1

    failures_per_other_affected_rack_list = []
    for i in affected_racks:
        if i != curr_rackId:
            failures_per_other_affected_rack_list.append(len(state.racks[i].failed_disks))
    
    # logging.info("failures_per_other_affected_rack_list: {}".format(failures_per_other_affected_rack_list))
    
    priority_cases = stripe_fail_cases(state.sys.top_n-1, priority-1, state.sys.num_disks_per_rack, failures_per_other_affected_rack_list, 
                                        state.sys.num_racks - 1 - len(failures_per_other_affected_rack_list))
    total_cases = stripe_total_cases(state.sys.top_n-1, state.sys.num_racks-1, state.sys.num_disks_per_rack)
    return priority_cases / total_cases

total_cases_dict = {}
def stripe_total_cases(num_chunks_in_stripe, num_other_racks, drives_per_rack):
    key = (num_chunks_in_stripe, num_other_racks, drives_per_rack)
    if key in total_cases_dict:
        return total_cases_dict[key]
    if num_chunks_in_stripe > num_other_racks:
        total_cases_dict[key] = 0 
        return 0
    else:
        total_cases_dict[key] = math.comb(num_other_racks, num_chunks_in_stripe) * (drives_per_rack ** num_chunks_in_stripe)
        return total_cases_dict[key]


stripe_fail_cases_dict = {}
def stripe_fail_cases(num_chunks_in_stripe, num_failures, drives_per_rack, failures_per_other_affected_rack_list, num_healthy_racks):
    key = (num_chunks_in_stripe, num_failures, drives_per_rack, tuple(failures_per_other_affected_rack_list), num_healthy_racks)
    if key in stripe_fail_cases_dict:
        return stripe_fail_cases_dict[key]
    
    num_other_affected_racks = len(failures_per_other_affected_rack_list)
    num_other_racks = num_other_affected_racks + num_healthy_racks
    if num_other_racks < num_chunks_in_stripe:
        return 0
    if num_failures > num_chunks_in_stripe:
        return 0
    if num_failures > num_other_affected_racks:
        return 0
    if num_failures < 0:
        return 0
    
    if num_other_affected_racks == 0:    # then must have num_failures = 0
        count = math.comb(num_healthy_racks, num_chunks_in_stripe) * (drives_per_rack ** num_chunks_in_stripe)
        stripe_fail_cases_dict[key] = count
        return count
    else:
        failed_disks_first_rack = failures_per_other_affected_rack_list[0]
        failures_per_other_affected_rack_subset = failures_per_other_affected_rack_list[1:]
        count = (failed_disks_first_rack * stripe_fail_cases(
                        num_chunks_in_stripe-1, num_failures-1, drives_per_rack, failures_per_other_affected_rack_subset, num_healthy_racks)
            + (drives_per_rack - failed_disks_first_rack) * stripe_fail_cases(
                        num_chunks_in_stripe-1, num_failures, drives_per_rack, failures_per_other_affected_rack_subset, num_healthy_racks)
            + stripe_fail_cases(num_chunks_in_stripe, num_failures, drives_per_rack, failures_per_other_affected_rack_subset, num_healthy_racks))
        stripe_fail_cases_dict[key] = count
        return count


if __name__ == "__main__":
    # imagine we have 50 racks and 2 disk failures
    # test 8+2
    n = 10
    num_disks_per_rack = 1
    failures_per_other_affected_rack_list = [1]
    num_racks = 50
    r = 50 # len(state.racks)
    B = 1 #len(state.disks)/len(state.racks)
    n = 10 #state.n
    disk = Disk(0, 20, 0)
    disk.priority = 2
    
    for priority in range(1,3):
        print('\npriority{}:'.format(priority))
        print('using generialized function....')
        priority_cases = stripe_fail_cases(n-1, priority-1, num_disks_per_rack, failures_per_other_affected_rack_list, 
                                            num_racks - 1 - len(failures_per_other_affected_rack_list))
        total_cases = stripe_total_cases(n-1, num_racks-1, num_disks_per_rack)
        
        print(priority_cases)
        print(total_cases)
        print(priority_cases / total_cases)



