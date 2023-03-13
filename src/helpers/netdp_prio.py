import numpy as np
import logging
import math

# import sys
# sys.path.append('..')
# from components.disk import Disk

# Scenario
# - one failure
# - located on one rack
# - disks with priority of 1
def f1p1(r,B,n):
    return 1

# Scenario
# - two failures
# - located on the same rack
# - disks with priority of 1
def f2p1_same(r,B,n):
    return 1

# Scenario
# - two failures
# - located on distinct rack
# - disks with priority of 1
def f2p1_distinct(r,B,n):
    a = (B-1)/B
    b = (n-1)/(r-1)
    c = (r-n)/(r-1)
    
    return a*b+c

# Scenario
# - two failures
# - located on distinct rack
# - disks with priority of 2
def f2p2_distinct(r,B,n):
    return fnpn_distinct(r,B,n,2)

# Scenario
# - three failures
# - located on the same rack
# - disks with priority of 1
def f3p1_same(r,B,n):
    return 1

# Scenario
# - three failures
# - 1 failure on one rack, and 2 failures on another
# - disk on the rack with 1 failure
# - disk with priority of 1
def f3p1_rack_with_1(r,B,n):
    a = (n-1)/(r-1)
    b = (B-2)/(B)
    c = (r-n)/(r-1)
    
    return a*b+c

# Scenario
# - three failures
# - 1 failure on one rack, and 2 failures on another
# - disk on the rack with 1 failure
# - disk with priority of 2
def f3p2_rack_with_1(r,B,n):
    a = (n-1)/(r-1)
    b = 2/B
    
    return a*b

# Scenario
# - three failures
# - 1 failure on one rack, and 2 failures on another
# - disk on the rack with 2 failure
# - disk with priority of 1
def f3p1_rack_with_2(r,B,n):
    a = (n-1)/(r-1)
    b = (B-1)/B
    c = (r-n)/(r-1)
    
    return a*b+c

# Scenario
# - three failures
# - 1 failure on one rack, and 2 failures on another
# - disk on the rack with 2 failure
# - disk with priority of 2
def f3p2_rack_with_2(r,B,n):
    a = (n-1)/(r-1)
    b = 1/B
    
    return a*b

# Scenario
# - thee failures
# - located on 3 distinct racks
# - disk with priority of 1
def f3p1_distinct(r,B,n):
    base = (r-1)*(r-2)
    a = ((r-n)*(r-n-1))/base
    b1 = (2*(r-n)*(n-1))/base
    b2 = (B*(B-1))/(B**2)
    c1 = ((n-1)*(n-2))/base
    c2 = ((B-1)**2)/(B**2)
    
    return a + b1*b2 + c1*c2

# Scneario
# - three failures
# - located on 3 distinct racks
# - disk with priority of 2
def f3p2_distinct(r,B,n):
    base = (r-1)*(r-2)
    a1 = ((r-n)*(n-1))/base
    a2 = (2*B)/(B**2)
    b1 = ((n-1)*(n-2))/base
    b2 = (2*(B-1))/(B**2)
    
    return a1*a2 + b1*b2

# Scenario
# - 3 failures
# - located on 3 distinct racks
# - disks with priority of 3
def f3p3_distinct(r,B,n):
    return fnpn_distinct(r,B,n,3)
    
# Scenario
# - f failures
# - located on distinct racks
# - disks with priority of f
def fnpn_distinct(r,B,n,f):
    a = 1/(B**(f-1))
    prod_i = np.arange(1, f)
    b_num = np.prod(n-prod_i)
    b_denom = np.prod(r-prod_i)
    logging.info("a: %s, b_num: %s, b_denom: %s", a, b_num, b_denom)
    
    return a*(b_num/b_denom)

def two_failure(r, B, n, disk,failed_disk_per_rack, priority):
    logging.info("Calling two_failure()")
    if (priority == 1):
        logging.info("f2p1_same()")
        return f2p1_same(r,B,n)
    elif (priority == 2):
        # Check the priority of the disk
        prio = disk.priority
        if (prio == 1):
            logging.info("f2p1_distinct()")
            return f2p1_distinct(r,B,n)
        elif (prio == 2):
            logging.info("f2p2_distinct()")
            return f2p2_distinct(r,B,n)
    else:
        raise Exception("Unknown failure state")
    
def three_failure(r, B, n, disk,failed_disk_per_rack, priority):
    logging.info("Calling three_failure()")
    if (priority == 1):
        return f3p1_same(r,B,n)
    elif (priority == 2):
        prio = disk.priority
        rack = disk.rackId
        if (len(failed_disk_per_rack[rack]) == 1):
            # Priority calculation for rack with 1 failure
            if (prio == 1):
                logging.info("Calling f3p1_rack_with_1()")
                return f3p1_rack_with_1(r,B,n)
            elif (prio == 2):
                logging.info("Calling f3p2_rack_with_1()")
                return f3p2_rack_with_1(r,B,n)
            else:
                raise Exception("Unknown failure state")
        elif (len(failed_disk_per_rack[rack]) == 2):
            # Priority calculation for rack with 2 failures
            if (prio == 1):
                logging.info("Calling f3p1_rack_with_2()")
                return f3p1_rack_with_2(r,B,n)
            elif (prio == 2):
                logging.info("Calling f3p2_rack_with_2()")
                return f3p2_rack_with_2(r,B,n)
            else:
                raise Exception("Unknown failure state")
        else:
            raise Exception("Unkown failure state")
    elif (priority == 3):
        prio = disk.priority
        if (prio == 1):
            logging.info("Calling f3p1_distinct()")
            return f3p1_distinct(r,B,n)
        elif (prio == 2):
            logging.info("Calling f3p2_distinct()")
            return f3p2_distinct(r,B,n)
        elif (prio == 3):
            logging.info("Calling f3p3_distinct()")
            return f3p3_distinct(r,B,n)
        else:
            raise Exception("Unkonwn failure state")
    else:
        raise Exception("Unknown failure state")
        
    
def priority_percent(state, disk,failed_disk_per_rack, max_priority, priority):
    r = len(state.racks)
    B = len(state.disks)/len(state.racks)
    n = state.n
    
    # print(f"Calculating pp for r={r}, B={B}, n={n}, failed_racks={failed_racks}")
    # Determine how many failures are there in the system
    
    # logging.info("(r,B,n,failed_racks,max_prio,prio): (%s,%s,%s,%s,%s,%s)", r, B, n, priority, max_priority, disk.priority)
    
    if (max_priority == 1):
        return f1p1(r,B,n)
    elif (max_priority == 2):
        return two_failure(r, B, n, disk, failed_disk_per_rack, priority)
    elif (max_priority == 3):
        return three_failure(r, B, n, disk, failed_disk_per_rack, priority)
    elif (max_priority == disk.priority):
        return fnpn_distinct(r,B,n,priority)
    else:
        raise Exception("Unknown failure state")



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

        print()
        print('using hardcoded function...')
        res = two_failure(r, B, n, disk, None, priority)
        print(res)



