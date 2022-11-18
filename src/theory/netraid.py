
##############################
# network-only slec clustered (network RAID)
##############################

# We divide the rack into multiple rack groups. Each group contains n=k+p racks.
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
    if num_affected_racks == 1:
        if num_failed_disks > drives_per_rack :
            return 0
        else
            survival_count_rack_group_dic[(drives_per_rack, num_failed_disks, 1)] = (
                        math.comb(drives_per_rack, num_failed_disks) * math.comb(num_racks, 1)
                    )
            return survival_count_rack_group_dic[(drives_per_rack, num_failed_disks, 1)]
        
    count = 0
    
    max_failures_per_diskgroup = min(p_net, num_failed_disks)
    # suppose g-th group has i disk failures
    for i in range(max_failures_per_diskgroup+1):
        # then group 1 to g-1 has f-i disk faiulres
        # suppose these f-i disk failures affects exactly j racks
        # then the g-th group affects r-j new racks
        # and the i disk failures and the j affected racks must overlap on i+j-r racks
        for j in range(num_affected_racks+1):
            if i+j-r > j or i+j-r < 0 or r-j > n_net-j or r-j < 0:
                continue
            count += math.comb(j, i+j-r) * math.comb(n_net-j, r-j) * survival_count_net_raid(
                            k_net, p_net, drives_per_rack-1,  num_failed_disks-i, j)
    survival_count_rack_group[drives_per_rack, num_failed_disks, num_affected_racks] = count
    return count



survival_count_dic = {}
def survival_count(k_net, p_net, num_rackgroups, drives_per_rack, num_failed_disks, num_affected_racks):
    if (num_rackgroups, num_failed_disks, num_affected_racks) in survival_count_dic:
        return survival_count_dic[(num_rackgroups, num_failed_disks, num_affected_racks)]

    n_net = k_net + p_net
    if num_failed_disks < num_affected_racks:
        return 0
    if num_failed_disks > num_affected_racks * (num_rackgroups * n_net):
        return 0
    if num_affected_racks == 0:
        # when reach here, we must have num_failed_disks == 0
        return 1
    
    max_failures_per_rackgroup = min(drives_per_rack*n_net, num_failed_disks)
    max_affected_racks_per_rackgroup = min(n_net, num_affected_racks)
    count = 0
    # suppose we have i disk failures in the rack group
    for i in range(max_failures_per_rackgroup+1):
        # suppose we have j affected racks in the rack group
        for j in range(max_affected_racks_per_rackgroup):
            if j > i:
                # we cannot have affected racks more than disk failures. So break, go to next i.
                break
            count += survival_count_rack_group(k_net, p_net, drives_per_rack, i, j) * (
                        survival_count(k_net, p_net, num_rackgroups-1, drives_per_rack, num_failed_disks-i, num_affected_racks-j)
                    )
    survival_count_dic[(num_rackgroups, num_failed_disks, num_affected_racks)] = count
    return count


