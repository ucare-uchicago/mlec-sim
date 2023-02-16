import math
import policies.total
# import total


# chunks_per_group: data_chunks + 1 local parity
# global_p: global parity #
# put one local group in a rack, or all global chunks in a rack
stripe_fail_cases_correlated_dict = {}
def stripe_fail_cases_correlated(num_groups, chunks_per_group, global_p, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks):
    key = (num_groups, chunks_per_group, global_p, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    # print(key)
    if key in stripe_fail_cases_correlated_dict:
        return stripe_fail_cases_correlated_dict[key]
    if num_failed_disks < num_affected_racks:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if num_failed_disks > num_affected_racks * drives_per_rack:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    num_global_groups = 1 if global_p > 0 else 0
    if num_racks < num_global_groups + num_groups or num_racks < num_affected_racks:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if num_failed_chunks > num_groups * chunks_per_group + global_p:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0

    count = 0
    if num_racks == 0:
        if num_failed_chunks > 0:
            return 0
        else:
            return 1
    
    max_failures_curr_rack = min(num_failed_disks-num_affected_racks+1, drives_per_rack, num_failed_disks)
    count = 0
    
    for curr_rack_failure in range(0, max_failures_curr_rack+1):
        curr_rack_affected = 0 if curr_rack_failure == 0 else 1
        safe_drives_curr_rack = drives_per_rack - curr_rack_failure
        # no chunk in this rack
        count += math.comb(drives_per_rack, curr_rack_failure) * stripe_fail_cases_correlated(num_groups, chunks_per_group, global_p, 
                            num_failed_chunks, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_failure, num_affected_racks-curr_rack_affected)
        # there is a local group in this rack:
        if num_groups > 0:
            max_failed_chunks_curr_rack = min(curr_rack_failure, chunks_per_group)
            max_safe_chunks_curr_rack = min(chunks_per_group, safe_drives_curr_rack)
            min_failed_chunks_curr_rack = chunks_per_group - max_safe_chunks_curr_rack
            for i in range(min_failed_chunks_curr_rack, max_failed_chunks_curr_rack+1):
                count += num_groups * (math.comb(drives_per_rack, curr_rack_failure) * math.comb(curr_rack_failure, i) 
                            * math.comb(safe_drives_curr_rack, chunks_per_group - i)
                            * stripe_fail_cases_correlated(num_groups-1, chunks_per_group, global_p, num_failed_chunks-i, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_failure, num_affected_racks-curr_rack_affected))
        print("curr_rack_failure: {}  count: {}".format(curr_rack_failure, count))
        # we put global parities in this rack:
        if global_p > 0:
            max_failed_chunks_curr_rack = min(curr_rack_failure, global_p)
            max_safe_chunks_curr_rack = min(global_p, safe_drives_curr_rack)
            min_failed_chunks_curr_rack = global_p - max_safe_chunks_curr_rack
            for i in range(min_failed_chunks_curr_rack, max_failed_chunks_curr_rack+1):
                count += (math.comb(drives_per_rack, curr_rack_failure) * math.comb(curr_rack_failure, i) 
                            * math.comb(safe_drives_curr_rack, global_p - i)
                            * stripe_fail_cases_correlated(num_groups, chunks_per_group, 0, num_failed_chunks-i, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_failure, num_affected_racks-curr_rack_affected))

        print("curr_rack_failure: {}  count: {}".format(curr_rack_failure, count))
    stripe_fail_cases_correlated_dict[key] = count
    # print(count)
    print("{}: {}".format(key, count))
    return count




def stripe_total_cases(k_net, p_net, total_drives, drives_per_rack):
    num_racks = total_drives // drives_per_rack
    n_net = k_net + p_net
    count =  math.comb(num_racks, n_net)
    count *= (drives_per_rack ** n_net)
    return count



if __name__ == "__main__":
    total_drives = 9
    num_failed_chunks = 2
    num_racks = 3
    drives_per_rack = 3
    num_failed_disks = 3
    num_affected_racks = 3
    num_groups = 2
    chunks_per_group = 2
    global_p = 1
    stripe_fail_cases = stripe_fail_cases_correlated(num_groups, chunks_per_group, global_p, num_failed_chunks, 
            num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    print(stripe_fail_cases)
    

