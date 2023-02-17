import math
import policies.total as total
# import total as total

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
    key = (num_failed_disks, num_diskgroups, p_local, disks_per_group)
    if key in survival_count_raid_single_rack:
        return survival_count_raid_single_rack[key]
    if num_failed_disks < 0 or num_failed_disks > p_local * num_diskgroups:
         return 0
    if num_diskgroups == 1:
        survival_count_raid_single_rack[key] = math.comb(disks_per_group, num_failed_disks)
        # print("{} {}".format(key, survival_count_raid_single_rack[key]))
        return survival_count_raid_single_rack[key]

    max_failures_per_group = min(num_failed_disks, p_local)
    count = 0
    for i in range(max_failures_per_group + 1):
        count += math.comb(disks_per_group, i) * raid_count_single_rack(num_failed_disks-i, num_diskgroups-1, p_local, disks_per_group)
    survival_count_raid_single_rack[key] = count
    # print("{} {}".format(key, count))
    return count


survival_count_system_dic = {}
def survival_count_system(k_net, p_net, k_local, p_local, total_racks, drives_per_rack, num_failed_disks, num_affected_racks):
    key = (k_net, p_net, k_local, p_local, total_racks, drives_per_rack, num_failed_disks, num_affected_racks)
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
    n_net = k_net + p_net
    num_diskgroups_per_rack = drives_per_rack // n_local

    max_failures_per_rack = min(num_failed_disks-num_affected_racks+1, drives_per_rack)
    count = 0
    for i in range(0, max_failures_per_rack + 1):
        if i == 0:
            cur_rack_affected = 0
        else:
            cur_rack_affected = 1
        cur_rack_survival_cases = raid_count_single_rack(i, num_diskgroups_per_rack, p_local, n_local)
        cur_rack_fail_cases = math.comb(drives_per_rack, i) - cur_rack_survival_cases
        # print("i: {}  cur_rack_survival_cases {}  cur_rack_fail_cases {}".format(i, cur_rack_survival_cases, cur_rack_fail_cases))
        count += (cur_rack_survival_cases * survival_count_system(
                            k_net, p_net, k_local, p_local, total_racks-1, 
                            drives_per_rack, num_failed_disks-i, num_affected_racks-cur_rack_affected)
                + cur_rack_fail_cases * survival_count_system(
                            k_net, p_net-1, k_local, p_local, total_racks-1, 
                            drives_per_rack, num_failed_disks-i, num_affected_racks-cur_rack_affected))
    survival_count_system_dic[key] = count
    # print("{} {}".format(key, count))
    return count




########################################################################################################################
#
#
#  the following considers stripe-level
#
#
########################################################################################################################



##############################
# suppose x disk failures in a rack with y raid disk groups
# how many cases will these x disk failures cause z disk groups to fail
raid_group_failures_count_single_rack_dic = {}
def raid_group_failures_count_single_rack(num_failed_disks, num_failed_diskgroups, num_diskgroups, p_local, disks_per_group):
    key = (num_failed_disks, num_failed_diskgroups, num_diskgroups, p_local, disks_per_group)
    if key in raid_group_failures_count_single_rack_dic:
        return raid_group_failures_count_single_rack_dic[key]
    if num_failed_disks < num_failed_diskgroups*(p_local+1) or num_failed_disks > disks_per_group * num_diskgroups:
        raid_group_failures_count_single_rack_dic[key] = 0
        return 0
    if num_failed_diskgroups > num_diskgroups:
        raid_group_failures_count_single_rack_dic[key] = 0
        return 0
    if num_failed_diskgroups < 0:
        raid_group_failures_count_single_rack_dic[key] = 0
        return 0
    if num_diskgroups == 0:     # when reach here, must have num_failed_diskgroups = 0 and num_failed_disks
        return 1

    max_failures_per_group = min(num_failed_disks, disks_per_group)
    count = 0
    for i in range(max_failures_per_group + 1):
        if i <= p_local:
            count += math.comb(disks_per_group, i) * raid_group_failures_count_single_rack(num_failed_disks-i, num_failed_diskgroups, num_diskgroups-1, p_local, disks_per_group)
        else:
            count += math.comb(disks_per_group, i) * raid_group_failures_count_single_rack(num_failed_disks-i, num_failed_diskgroups-1, num_diskgroups-1, p_local, disks_per_group)
    raid_group_failures_count_single_rack_dic[key] = count
    # print("{} {}".format(key, count))
    return count




stripe_fail_cases_correlated_dict = {}
def stripe_fail_cases_correlated(n_net, p_net, n_local, p_local, num_failed_chunkgroups, num_racks, drives_per_rack, num_failed_disks, num_affected_racks):
    key = (n_net, p_net, n_local, p_local, num_failed_chunkgroups, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    # print(key)
    if key in stripe_fail_cases_correlated_dict:
        return stripe_fail_cases_correlated_dict[key]
    if num_failed_disks < num_affected_racks or num_affected_racks < 0:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if num_failed_disks > num_affected_racks * drives_per_rack:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if num_racks < n_net or n_net < 0 or num_racks < num_affected_racks:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if n_local > drives_per_rack:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if num_failed_chunkgroups > n_net or num_failed_chunkgroups > num_affected_racks:
        stripe_fail_cases_correlated_dict[key] = 0
        return 0
    if num_failed_disks < num_failed_chunkgroups*(p_local+1):
        stripe_fail_cases_correlated_dict[key] = 0
        return 0

    count = 0
    # when num_racks == 0, based on our check above, when we reach here, we must have:
    # num_affected_racks = 0
    # num_failed_disks = 0
    # num_failed_chunkgroups <= 0
    # n_net = 0
    if num_racks == 0:  
        stripe_fail_cases_correlated_dict[key] = 1
        return 1
    
    max_failures_curr_rack = min(num_failed_disks-num_affected_racks+1, drives_per_rack)
    count = 0
    num_diskgroups = drives_per_rack // n_local
    
    for curr_rack_disk_failure in range(0, max_failures_curr_rack+1):
        curr_rack_affected = 0 if curr_rack_disk_failure == 0 else 1
        # if stripe doesn't reside in this rack
        count += math.comb(drives_per_rack, curr_rack_disk_failure) * stripe_fail_cases_correlated(n_net, p_net, n_local, p_local, 
                            num_failed_chunkgroups, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_disk_failure, num_affected_racks-curr_rack_affected)
        if n_net > 0:
            max_diskgroup_failures_in_rack = curr_rack_disk_failure // (p_local+1)      # cant be more than it
            for diskgroup_failures_in_rack in range(max_diskgroup_failures_in_rack+1):
                count += raid_group_failures_count_single_rack(curr_rack_disk_failure, diskgroup_failures_in_rack, num_diskgroups, p_local, n_local) * (
                        diskgroup_failures_in_rack * stripe_fail_cases_correlated(n_net-1, p_net, n_local, p_local, 
                            num_failed_chunkgroups-1, num_racks-1,  
                            drives_per_rack, num_failed_disks-curr_rack_disk_failure, num_affected_racks-curr_rack_affected) +
                        (num_diskgroups - diskgroup_failures_in_rack) * stripe_fail_cases_correlated(n_net-1, p_net, n_local, p_local, 
                            num_failed_chunkgroups, num_racks-1,  
                            drives_per_rack, num_failed_disks-curr_rack_disk_failure, num_affected_racks-curr_rack_affected)                     
                        )
    stripe_fail_cases_correlated_dict[key] = count
    # print(count)
    return count



def stripe_total_cases_correlated(n_net, p_net, n_local, p_local, num_racks, drives_per_rack, num_failed_disks, num_affected_racks):
    count = total.total_cases_fixed_racks(num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    count *=  math.comb(num_racks, n_net)
    diskgroups_per_rack = drives_per_rack // n_local
    count *= (diskgroups_per_rack ** n_net)
    return count



if __name__ == "__main__":
    # survival_cases = survival_count_mlec_group_k_p(3, 2, 3, 2, 12, 3)
    # total_cases = total.total_cases_fixed_racks(5, 5, 12, 3)
    # print("\ntotal: \t\t{} \nsurvival: \t{}".format(total_cases, survival_cases))
    # dl_prob = 1 - survival_cases / total_cases
    # print("dl prob: \t{}\n".format(dl_prob))    # brute force is 0.9340659341. Result should match
    # print(survival_count_mlec_group_dic)
    # for num_failed_disks in range(10,11):
    #     for num_affected_racks in range(10,11):
    #         # survival_cases = survival_count_rack_group(3, 2, 3, 2, 2, failed_disks, affected_racks)
    #         survival_cases = survival_count_system(9, 1, 9, 1, 40, 800, num_failed_disks, num_affected_racks)
    #         # survival_cases = survival_count_system(1, 0, 1, 1, 1, 4, 1, 1)
    #         total_cases = total.total_cases_fixed_racks(40, 800, num_failed_disks, num_affected_racks)
    #         print("\ntotal: \t\t{} \nsurvival: \t{}".format(total_cases, survival_cases))
    #         dl_prob = 1 - survival_cases / total_cases
    #         print("dl prob: \t{}\n".format(dl_prob))    # brute force is 0.9340659341. Result should match

    # verify raid_group_failures_count_single_rack
    # num_failed_disks = 4
    # num_failed_diskgroups = 0
    # num_diskgroups = 2
    # p_local = 2
    # disks_per_group = 4
    # count = raid_group_failures_count_single_rack(num_failed_disks, num_failed_diskgroups, num_diskgroups, p_local, disks_per_group)
    # print(count)

    # verify stripe_fail_cases_correlated

    n_net = 2
    p_net = 1
    n_local = 1
    p_local = 0
    num_failed_chunkgroups = 2
    num_racks = 2
    drives_per_rack = 4
    num_failed_disks = 4
    num_affected_racks = 2

    n_net = 10
    p_net = 1
    n_local = 1
    p_local = 0
    num_failed_chunkgroups = 3
    num_racks = 40
    drives_per_rack = 20
    num_failed_disks = 4
    num_affected_racks = 3

    count = stripe_fail_cases_correlated(n_net, p_net, n_local, p_local, num_failed_chunkgroups, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    print(count)