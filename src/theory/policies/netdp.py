import math
# import policies.total

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
    print(stripe_total_cases(8, 2, 40000, 1000))
    print(stripe_fail_cases(9, 1, 1000, [1,1,0,0,0,0,0,0,0,0], 0))
    

