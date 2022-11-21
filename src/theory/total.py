import math

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


if __name__ == "__main__":
    total_cases = total_cases_affect_all_racks(5, 8, 3)
    print("\ntotal: \t\t{}".format(total_cases))

