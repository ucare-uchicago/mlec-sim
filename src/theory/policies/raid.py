import math


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
    
# total number of cases for r racks to survive f disk failures in RAID.  Each rack should have at least 1 failure
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

# total number of cases survive f disk failures affecting exactly r racks.
def survival_count_raid_fixed_racks(k_local, p_local, num_racks, drives_per_rack, num_failed_disks, num_affected_racks):
    return math.comb(num_racks, num_affected_racks) * survival_count_raid(k_local, p_local, drives_per_rack, num_failed_disks, num_affected_racks)