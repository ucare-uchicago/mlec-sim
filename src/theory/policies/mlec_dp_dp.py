import math
import policies.total as total
# import total as total



survival_count_dp_single_rack = {}
def dp_survival_count_single_rack(num_failed_disks, num_diskgroups, p_local, disks_per_group):
    key = (num_failed_disks, num_diskgroups, p_local, disks_per_group)
    if key in survival_count_dp_single_rack:
        return survival_count_dp_single_rack[key]
    if num_failed_disks < 0 or num_failed_disks > p_local * num_diskgroups:
         return 0
    if num_diskgroups == 1:
        survival_count_dp_single_rack[key] = math.comb(disks_per_group, num_failed_disks)
        # print("{} {}".format(key, survival_count_dp_single_rack[key]))
        return survival_count_dp_single_rack[key]

    max_failures_per_group = min(num_failed_disks, p_local)
    count = 0
    for i in range(max_failures_per_group + 1):
        count += math.comb(disks_per_group, i) * dp_survival_count_single_rack(num_failed_disks-i, num_diskgroups-1, p_local, disks_per_group)
    survival_count_dp_single_rack[key] = count
    # print("{} {}".format(key, count))
    return count


survival_count_system_dic = {}
def survival_count_system(k_net, p_net, k_local, p_local, total_racks, drives_per_rack, drives_per_localgroup, num_failed_disks, num_affected_racks):
    key = (k_net, p_net, k_local, p_local, total_racks, drives_per_rack, drives_per_localgroup, num_failed_disks, num_affected_racks)
    if key in survival_count_system_dic:
        return survival_count_system_dic[key]
    if total_racks < num_affected_racks:
        return 0
    if p_net < 0:
        return 0
    if num_failed_disks < num_affected_racks:
        return 0
    if num_failed_disks > num_affected_racks * drives_per_rack:
        return 0
    if num_affected_racks == 0:
        # when reach here, we must have num_failed_disks == 0
        return 1

    n_local = k_local + p_local
    assert drives_per_localgroup >= n_local, "drives_per_localgroup smaller than n_local, which is impossible!"
    n_net = k_net + p_net
    num_diskgroups_per_rack = drives_per_rack // drives_per_localgroup

    max_failures_per_rack = min(num_failed_disks-num_affected_racks+1, drives_per_rack)
    count = 0
    for i in range(0, max_failures_per_rack + 1):
        if i == 0:
            cur_rack_affected = 0
        else:
            cur_rack_affected = 1
        cur_rack_survival_cases = dp_survival_count_single_rack(i, num_diskgroups_per_rack, p_local, drives_per_localgroup)
        cur_rack_fail_cases = math.comb(drives_per_rack, i) - cur_rack_survival_cases
        # print("i: {}  cur_rack_survival_cases {}  cur_rack_fail_cases {}".format(i, cur_rack_survival_cases, cur_rack_fail_cases))
        count += (cur_rack_survival_cases * survival_count_system(
                            k_net, p_net, k_local, p_local, total_racks-1, 
                            drives_per_rack, drives_per_localgroup, num_failed_disks-i, num_affected_racks-cur_rack_affected)
                + cur_rack_fail_cases * survival_count_system(
                            k_net, p_net-1, k_local, p_local, total_racks-1, 
                            drives_per_rack, drives_per_localgroup, num_failed_disks-i, num_affected_racks-cur_rack_affected))
    survival_count_system_dic[key] = count
    # print("{} {}".format(key, count))
    return count




if __name__ == "__main__":
    dp_survival_count_per_rack = dp_survival_count_single_rack(2,2,2,10)
    print(dp_survival_count_per_rack)