import math
import policies.total
import random
# import total



stripe_fail_cases_correlated_dict = {}
def stripe_fail_cases_correlated(n_net, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks):
    key = (n_net, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
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
    
    max_failures_curr_rack = min(num_failed_disks-num_affected_racks+1, drives_per_rack, num_failed_disks)
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



stripe_fixed_fail_chunk_cases_correlated_dict = {}
def stripe_fixed_fail_chunk_cases_correlated(n_net, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks):
    key = (n_net, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    
    if key in stripe_fixed_fail_chunk_cases_correlated_dict:
        return stripe_fixed_fail_chunk_cases_correlated_dict[key]
    if num_failed_disks < num_affected_racks:
        stripe_fixed_fail_chunk_cases_correlated_dict[key] = 0
        return 0
    if num_failed_disks > num_affected_racks * drives_per_rack:
        stripe_fixed_fail_chunk_cases_correlated_dict[key] = 0
        return 0
    if num_racks < n_net or num_racks < num_affected_racks:
        stripe_fixed_fail_chunk_cases_correlated_dict[key] = 0
        return 0
    if num_failed_chunks > n_net or num_failed_chunks < 0:
        stripe_fixed_fail_chunk_cases_correlated_dict[key] = 0
        return 0
    

    count = 0
    if num_racks == 1:
        if n_net == 0:
            count =  math.comb(drives_per_rack, num_failed_disks)
        else:  # n_net = 1
            if num_failed_chunks == 1:
                count = num_failed_disks * math.comb(drives_per_rack, num_failed_disks)
            else:
                # print(key)
                # print("drives_per_rack {} . num_failed_disks {}".format(drives_per_rack, num_failed_disks))
                count = (drives_per_rack - num_failed_disks) * math.comb(drives_per_rack, num_failed_disks)
        stripe_fixed_fail_chunk_cases_correlated_dict[key] = count
        # print(key)
        # print(count)
        return count
    
    max_failures_curr_rack = min(num_failed_disks-num_affected_racks+1, drives_per_rack, num_failed_disks)
    count = 0
    
    for curr_rack_failure in range(0, max_failures_curr_rack+1):
        curr_rack_affected = 0 if curr_rack_failure == 0 else 1
        count += math.comb(drives_per_rack, curr_rack_failure) * stripe_fixed_fail_chunk_cases_correlated(n_net, num_failed_chunks, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_failure, num_affected_racks-curr_rack_affected)
        if n_net > 0:
            count += math.comb(drives_per_rack, curr_rack_failure) * (
                        curr_rack_failure * stripe_fixed_fail_chunk_cases_correlated(n_net-1, num_failed_chunks-1, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_failure, num_affected_racks-curr_rack_affected) +
                        (drives_per_rack - curr_rack_failure) * stripe_fixed_fail_chunk_cases_correlated(n_net-1, num_failed_chunks, num_racks-1, 
                            drives_per_rack, num_failed_disks-curr_rack_failure, num_affected_racks-curr_rack_affected)                        
                        )
    stripe_fixed_fail_chunk_cases_correlated_dict[key] = count
    # print(key)
    # print("\t{}".format(count))
    return count


recoverability_dict = {}
#function to calculate recoverability
def calculate_recoverability(k, l, r, R):
    key = (k, l, r, R)
    if key in recoverability_dict:
        return recoverability_dict[key]

    n = k + l + r

    #create k data blocks
    data_blocks = []
    for i in range(k):
        data_blocks.append('k'+str(i))


    #create l local parity blocks
    local_parities = []
    for i in range(l):
        local_parities.append('l'+str(i))


    #create r global parity blocks
    global_parities = []
    for i in range(r):
        global_parities.append('r'+str(i))


    #here is the whole set of data and parity blocks
    data_and_parity = data_blocks + local_parities + global_parities

    print(data_and_parity)

    
    #variables required to group data blocks into l local groups
    num_local_groups = l
    local_groups = []
    local_group_ids = []
    
    #group data blocks into l groups
    start = 0
    end = k
    step = int(k/l)
    for i in range(start, end, step):
        x = i
        local_groups.append(data_blocks[x:x+step])
    
    
    #setting initial recoverable patterns to 0
    recoverable = 0
    
    iters=100000
    #start the recoverability calculation
    for i in range(iters):
        k_terms = 0
        l_terms = l
        r_terms = r
        random_comb = random.sample(data_and_parity, R)
        comb_list = list(random_comb)

        
        #find number of data blocks in a failure combo/pattern
        for j in range(len(random_comb)):
            if 'k' in comb_list[j]:
                k_terms = k_terms + 1

        
        #check if failed data block can be swapped with its local group parity
        #calculate the number of failed data blocks and available global parity blocks
        for j in range(len(random_comb)):
            if 'k' in random_comb[j]:
                for m in range(len(local_groups)):
                    if random_comb[j] in local_groups[m]:
                        local_group_id = m
                        break
                if local_parities[local_group_id] not in comb_list:
                    k_terms = k_terms - 1
                    comb_list.append(local_parities[local_group_id])
            if 'r' in random_comb[j]:
                r_terms = r_terms - 1
        if k_terms <= r_terms:
            recoverable = recoverable + 1
        
    res = recoverable/iters
    recoverability_dict[key] = res
    return res



def stripe_total_cases(k_net, p_net, total_drives, drives_per_rack):
    num_racks = total_drives // drives_per_rack
    n_net = k_net + p_net
    count =  math.comb(num_racks, n_net)
    count *= (drives_per_rack ** n_net)
    return count



if __name__ == "__main__":
    n_net = 10
    total_drives = 1100
    total_cases = stripe_total_cases(8, 2, total_drives, 100)
    # failure_cases = stripe_fail_cases(10, 3, 100, [2,1,1,0,0,0,0,0,0,0], 0)
    num_failed_chunks = 4
    num_racks = 11
    drives_per_rack = 100
    num_failed_disks = 5
    num_affected_racks = 5
    failure_cases_fixed_chunks = stripe_fixed_fail_chunk_cases_correlated(n_net, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    failure_cases_1 = stripe_fail_cases_correlated(n_net, num_failed_chunks, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    failure_cases_2 = stripe_fail_cases_correlated(n_net, num_failed_chunks+1, num_racks, drives_per_rack, num_failed_disks, num_affected_racks)
    print("total cases: \t\t\t{}\nfailure_cases_fixed_chunks: \t{}".format(total_cases, failure_cases_fixed_chunks))
    print("failure_cases_1: \t\t{}\nfailure_cases_2: \t\t{}\ndiff: \t\t\t\t{}".format(failure_cases_1, failure_cases_2, failure_cases_1 - failure_cases_2))
    stripe_failure_prob = failure_cases / total_cases
    print("stripe fail prob: \t{}".format(stripe_failure_prob))

    chunks_per_drive = 100
    num_stripes = chunks_per_drive * total_drives // n_net    
    no_failure_prob = (1-stripe_failure_prob) ** num_stripes
    print("system fail prob: {}".format(1-no_failure_prob))
    

