import math
# import policies.total



stripe_fail_cases_correlated_dict = {}
def stripe_fail_cases_correlated(n_net, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks):
    key = (n_net, num_failed_chunks, num_racks, num_failed_disks, num_affected_racks)
    # print(key)
    if key in stripe_fail_cases_correlated_dict:
        return stripe_fail_cases_correlated_dict[key]
    if num_failed_disks < num_affected_racks:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if num_failed_disks > num_affected_racks * drives_per_rack:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if num_racks < n_net or num_racks < num_affected_racks:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if num_failed_chunks > n_net:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0

    count = 0
    if num_racks == 1:
        if n_net == 0:
            count =  math.comb(drives_per_rack, num_failed_disks)
        else:  # n_net = 1
            if num_failed_chunks == 1:
                count = num_failed_disks * math.comb(drives_per_rack, num_failed_disks)
            else:
                count = drives_per_rack * math.comb(drives_per_rack, num_failed_disks)
        stripe_fail_cases_correlated_dict[key] = count
        # print(count)
        return count
    
    max_failures_curr_rack = min(num_failed_disks-num_affected_racks+1, drives_per_rack)
    count = 0
    
    for curr_rack_failure in range(0, max_failures_curr_rack+1):
        curr_rack_affected = 0 if curr_rack_failure == 0 else 1
        count += math.comb(drives_per_rack, curr_rack_failure) * stripe_fail_cases_correlated(n_net, num_failed_chunks, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_failure, num_affected_racks-curr_rack_affected)
        if n_net > 0:
            count += math.comb(drives_per_rack, curr_rack_failure) * (
                        curr_rack_failure * stripe_fail_cases_correlated(n_net-1, num_failed_chunks-1, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_failure, num_affected_racks-curr_rack_affected) +
                        (drives_per_rack - curr_rack_failure) * stripe_fail_cases_correlated(n_net-1, num_failed_chunks, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_failure, num_affected_racks-curr_rack_affected)                        
                        )
    stripe_fail_cases_correlated_dict[key] = count
    # print(count)
    return count



stripe_fail_cases_dict = {}
def stripe_fail_cases(n_net, num_failures, drives_per_rack, failures_per_rack_list, curr_rack):
    num_remaining_racks = len(failures_per_rack_list) - curr_rack
    failures_curr_rack = failures_per_rack_list[curr_rack]
    if num_remaining_racks < n_net:
        return 0
    if num_failures > n_net:
        return 0
    if num_remaining_racks == 1:
        if n_net <= 0:
            return 1
        else:
            if num_failures == 1:
                return failures_curr_rack
            else:
                return drives_per_rack
    if (n_net, num_failures, curr_rack) in stripe_fail_cases_dict:
        return stripe_fail_cases_dict[n_net, num_failures, curr_rack]
    else:
    
        res = (failures_curr_rack * stripe_fail_cases(n_net-1, num_failures-1, drives_per_rack, failures_per_rack_list, curr_rack+1)
            + (drives_per_rack - failures_curr_rack) * stripe_fail_cases(n_net-1, num_failures, drives_per_rack, failures_per_rack_list, curr_rack+1)
            + stripe_fail_cases(n_net, num_failures, drives_per_rack, failures_per_rack_list, curr_rack+1))
        stripe_fail_cases_dict[n_net, num_failures, curr_rack] = res
        return res


def stripe_total_cases(k_net, p_net, total_drives, drives_per_rack):
    num_racks = total_drives // drives_per_rack
    n_net = k_net + p_net
    count =  math.comb(num_racks, n_net)
    count *= (drives_per_rack ** n_net)
    return count



if __name__ == "__main__":
    n_net = 10
    total_drives = 1000
    total_cases = stripe_total_cases(8, 2, 1000, 100)
    failure_cases = stripe_fail_cases(10, 3, 100, [2,1,1,0,0,0,0,0,0,0], 0)
    print("total cases: \t{}\nfailure_cases: \t{}".format(total_cases, failure_cases))
    stripe_failure_prob = failure_cases / total_cases
    print("stripe fail prob: \t{}".format(stripe_failure_prob))

    chunks_per_drive = 100
    num_stripes = chunks_per_drive * total_drives // n_net    
    no_failure_prob = (1-stripe_failure_prob) ** num_stripes
    print("system fail prob: {}".format(1-no_failure_prob))
    

