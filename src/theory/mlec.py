import math
import total

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
# num_racks: n_n
# tole_racks: max number of failed racks that can be tolerated. Should be p_n
# NOTE: Here it's fine for tole_racks > num_racks. 
#       For the dynamic programming purpose, tole_racks just means the tolerance upper bound.
#       However, in reality, usually we have p_n < n_n
survival_count_mlec_group_dic = {}
def survival_count_mlec_group(num_racks, tole_racks, k_local, p_local, num_failed_disks, num_affected_racks):
    # print((num_racks, tole_racks, k_local, p_local, num_failed_disks, num_affected_racks))
    if (num_racks, tole_racks, k_local, p_local, num_failed_disks, num_affected_racks) in survival_count_mlec_group_dic:
        return survival_count_mlec_group_dic[(num_racks, tole_racks, k_local, p_local, num_failed_disks, num_affected_racks)]
    # corner cases
    if num_racks < 0 or tole_racks < 0 or num_failed_disks < 0 or num_affected_racks < 0:
        return 0
    # cannot have more racks than disks
    if num_failed_disks < num_affected_racks:
        return 0
    # cannot affect more than n_n racks
    if num_affected_racks > num_racks:
        return 0
    # base cases
    if num_affected_racks == 0:
        if num_failed_disks == 0:
            survival_count_mlec_group_dic[(num_racks, tole_racks, k_local, p_local, num_failed_disks, num_affected_racks)] = 1
        else:
            survival_count_mlec_group_dic[(num_racks, tole_racks, k_local, p_local, num_failed_disks, num_affected_racks)] = 0
        return survival_count_mlec_group_dic[(num_racks, tole_racks, k_local, p_local, num_failed_disks, num_affected_racks)]
    # recurrence relation
    count = 0
    n_local = k_local + p_local
    for i in range(n_local + 1):
        if i == 0:
            count += survival_count_mlec_group(num_racks-1, tole_racks, k_local, p_local, num_failed_disks, num_affected_racks)
        elif i <= p_local:
            count += math.comb(n_local, i) * survival_count_mlec_group(num_racks-1, tole_racks, k_local, p_local, num_failed_disks-i, num_affected_racks-1)
        else:
            count += math.comb(n_local, i) * survival_count_mlec_group(num_racks-1, tole_racks-1, k_local, p_local, num_failed_disks-i, num_affected_racks-1)
    survival_count_mlec_group_dic[(num_racks, tole_racks, k_local, p_local, num_failed_disks, num_affected_racks)] = count
    return count


# Report number of survival instances with parameters k_net and p_net.
def survival_count_mlec_group_k_p(k_net, p_net, k_local, p_local, num_failed_disks, num_affected_racks):
    return survival_count_mlec_group(k_net+p_net, p_net, k_local, p_local, num_failed_disks, num_affected_racks)


# -- Step 2:
#    For each rack group, consider how many survival instances if the rack group has f disk failures affecting r racks.
# 
#    racks:   r1  |  r2 | r3
#             --  | --- | --
#     mlec    d11 | d21 | d31
#     group   d12 | d22 | d32
#       1     d13 | d23 | d33
#             --- | --- | ---
#      mlec   d14 | d24 | d34
#     group   d15 | d25 | d35
#       2     d16 | d26 | d36
# 
# We divide the rack group into multiple mlec groups. 
# The recurrence relation comes by considering what will happen if some mlec group has i disk failures affecting j racks.
survival_count_rack_group_dic = {}
def survival_count_rack_group(k_net, p_net, k_local, p_local, num_mlec_groups, num_failed_disks, num_affected_racks):
    if (k_net, p_net, k_local, p_local, num_mlec_groups, num_failed_disks, num_affected_racks) in survival_count_rack_group_dic:
        return survival_count_rack_group_dic[(k_net, p_net, k_local, p_local, num_mlec_groups, num_failed_disks, num_affected_racks)]
    if num_failed_disks == 0 and num_affected_racks == 0 and num_mlec_groups >= 0:
        survival_count_rack_group_dic[(k_net, p_net, k_local, p_local, num_mlec_groups, num_failed_disks, num_affected_racks)] = 1
        return 1
    if num_mlec_groups <= 0 or num_failed_disks < 0 or num_affected_racks < 0:
        return 0
    if num_affected_racks > num_failed_disks:
        return 0
    if num_failed_disks > num_affected_racks * num_mlec_groups * (k_local + p_local):
        return 0

    n_net = k_net + p_net
    r = num_affected_racks
    count = 0
    for i in range(num_failed_disks+1):
        for j in range(num_affected_racks+1):
            for h in range(num_affected_racks+1):
                # suppose j racks are affected in the target mlec group, h racks are affected in the rest mlec groups.
                # divide the n_n racks into 2 parts. Part 1 are the h racks affected by other mlec groups. Part 2 are the n_n-h racks not affected by other mlec groups.
                # then the target mlec group should affect (j+h-r) racks in part 1, and (r-h) racks in part 2.
                # that is to say, j and h should overlap (j+h-r) racks. And the target mlec group should affect another (r-h) racks.
                if h<j+h-r or j+h-r<0 or n_net-h<r-h or r-h<0:
                    continue
                count += (math.comb(h, j+h-r) * math.comb(n_net-h, r-h)
                            * survival_count_mlec_group(j, p_net, k_local, p_local, i, j)
                            * survival_count_rack_group(k_net, p_net, k_local, p_local, num_mlec_groups-1, num_failed_disks-i, h))
    survival_count_rack_group_dic[(k_net, p_net, k_local, p_local, num_mlec_groups, num_failed_disks, num_affected_racks)] = count
    return count



if __name__ == "__main__":
    # survival_cases = survival_count_mlec_group_k_p(3, 2, 3, 2, 12, 3)
    # total_cases = total.total_cases_fixed_racks(5, 5, 12, 3)
    # print("\ntotal: \t\t{} \nsurvival: \t{}".format(total_cases, survival_cases))
    # dl_prob = 1 - survival_cases / total_cases
    # print("dl prob: \t{}\n".format(dl_prob))    # brute force is 0.9340659341. Result should match
    # print(survival_count_mlec_group_dic)
    for failed_disks in range(3,21):
        for affected_racks in range(3,4):
            survival_cases = survival_count_rack_group(3, 2, 3, 2, 2, failed_disks, affected_racks)
            total_cases = total.total_cases_fixed_racks(5, 10, failed_disks, affected_racks)
            print("\ntotal: \t\t{} \nsurvival: \t{}".format(total_cases, survival_cases))
            dl_prob = 1 - survival_cases / total_cases
            print("dl prob: \t{}\n".format(dl_prob))    # brute force is 0.9340659341. Result should match
