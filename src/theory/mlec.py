import math

##############################
# mlec clustered
##############################

# We divide the rack into multiple rack groups. Each group contains n_n racks.
# We further divide a rack group into multiple mlec groups.
# Each mlec group is composed of n_n*n_l disks.
# An mlec group spread among n_n racks, with n_l disks in each rack.
# -- Step 1:
#    For each mlec group, consider how many survival instances if f failures affect r racks, 
#    with no more than p_n racks having more than p_l failures.
#    racks:   r1  |  r2 | r3
#             --  | --- | --
#             d11 | d21 | d31
#             d12 | d22 | d32
#             d13 | d23 | d33
survival_count_mlec_group_dic = {}
def survival_count_mlec_group(k_net, p_net, k_local, p_local, num_failed_disks, num_affected_racks):
    if (k_net, p_net, k_local, p_local, num_failed_disks, num_affected_racks) in survival_count_mlec_group_dic:
        return survival_count_mlec_group_dic[(k_net, p_net, k_local, p_local, num_failed_disks, num_affected_racks)]
    # corner cases
    if k_net < 0 or p_net < 0 or num_failed_disks-i < 0 or num_affected_racks-1 < 0:
        return 0
    # cannot have more racks than disks
    if num_failed_disks < num_affected_racks:
        return 0
    # cannot affect more than n_n racks
    n_local = k_local + p_local
    n_net = k_net + p_net
    if num_affected_racks > n_net:
        return 0
    # base cases
    if num_failed_disks == 0:
        return 1
    if num_affected_racks == 0:
        return 0
    count = 0
    for i in range(n_local + 1):
        if i == 0:
            count += survival_count_mlec_group(k_net-1, p_net, k_local, p_local, num_failed_disks, num_affected_racks)
        elif i <= p_local:
            count += math.comb(n_local, i) * survival_count_mlec_group(k_net-1, p_net, k_local, p_local, num_failed_disks-i, num_affected_racks-1)
        else:
            count += math.comb(n_local, i) * survival_count_mlec_group(k_net, p_net-1, k_local, p_local, num_failed_disks-i, num_affected_racks-1)
    survival_count_mlec_group_dic[(k_net, p_net, k_local, p_local, num_failed_disks, num_affected_racks)] = count
    return count





# For each rack group, consider how many survival instances if the rack group has r rack failures
# ------
# Count survival instances for a single rack if there are f failures in r racks.
# The rack group is further divided into disk groups
# each disk group contains n=k+p disks.
# each disk group has exactly one disk in each rack
# The layout looks like this for SLEC2+1:
# 
# racks:   r1 | r2 | r3
#          -- | -- | --
# group A: d1 | d2 | d3
# group B: d1 | d2 | d3
# group C: d1 | d2 | d3
# 
# We need to satisfy two constrains:
# 1). every group has at most p failures
# 2). there are exactly r affected racks
# 
# Given m racks and g disk groups, thus m*n disks in total, define s(f,r,g) as the number of instances for net-raid k+p
# to survive  f disk failures in r affected racks
# here m = k_net + p_net
#      g =  drives_per_rack

survival_count_rack_group_dic = {}
def survival_count_rack_group(k_net, p_net, drives_per_rack, num_failed_disks, num_affected_racks):
    if (drives_per_rack, num_failed_disks, num_affected_racks) in survival_count_rack_group_dic:
        return survival_count_rack_group_dic[(drives_per_rack, num_failed_disks, num_affected_racks)]
    if num_affected_racks > num_failed_disks:
        return 0
    if num_failed_disks > drives_per_rack * num_affected_racks:
        return 0
    if num_affected_racks == 0:
        # when reach here, we must have num_failed_disks == 0
        return 1

    n_net = k_net + p_net
    num_racks = n_net
    if num_affected_racks == 1:
        if num_failed_disks > drives_per_rack :
            return 0
        else:
            survival_count_rack_group_dic[(drives_per_rack, num_failed_disks, 1)] = (
                        math.comb(drives_per_rack, num_failed_disks) * math.comb(num_racks, 1)
                    )
            return survival_count_rack_group_dic[(drives_per_rack, num_failed_disks, 1)]
        
    count = 0
    
    max_failures_per_diskgroup = min(p_net, num_failed_disks)
    # suppose g-th group has i disk failures
    r = num_affected_racks
    for i in range(max_failures_per_diskgroup+1):
        # then group 1 to g-1 has f-i disk faiulres
        # suppose these f-i disk failures affects exactly j racks
        # then the g-th group affects r-j new racks
        # and the i disk failures and the j affected racks must overlap on i+j-r racks
        for j in range(num_affected_racks+1):
            if i+j-r > j or i+j-r < 0 or r-j > n_net-j or r-j < 0:
                continue
            count += math.comb(j, i+j-r) * math.comb(n_net-j, r-j) * survival_count_rack_group(
                            k_net, p_net, drives_per_rack-1,  num_failed_disks-i, j)
    survival_count_rack_group_dic[(drives_per_rack, num_failed_disks, num_affected_racks)] = count
    return count


if __name__ == "__main__":
    survival_count_mlec_group(6, 1, 2, 1, num_failed_disks, num_affected_racks)