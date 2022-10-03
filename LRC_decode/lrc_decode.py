from math import comb
import itertools
import argparse

def calculate_recoverability(k, l, r, R):
    n = k + l + r

    data_blocks = []
    for i in range(k):
        data_blocks.append('k'+str(i))
    #print(data_blocks)


    local_parities = []
    for i in range(l):
        local_parities.append('l'+str(i))
    #print(local_parities)


    global_parities = []
    for i in range(r):
        global_parities.append('r'+str(i))
    #print(global_parities)

    data_and_parity = data_blocks + local_parities + global_parities
    #print(data_and_parity)
    
    all_comb = list(itertools.combinations(data_and_parity, R))
    
    num_local_groups = l
    local_groups = []

    local_group_ids = []
    
    start = 0
    end = k
    step = int(k/l)
    for i in range(start, end, step):
        x = i
        local_groups.append(data_blocks[x:x+step])
    
    #print(local_groups)
    recoverable = 0
    for i in range(len(all_comb)):
        k_terms = 0
        l_terms = l
        r_terms = r
        comb_list = list(all_comb[i])
        #print(comb_list)
        for j in range(len(all_comb[i])):
            if 'k' in comb_list[j]:
                k_terms = k_terms + 1
        #print("   ", k_terms, l_terms, r_terms)
        for j in range(len(all_comb[i])):
            if 'k' in all_comb[i][j]:
                for m in range(len(local_groups)):
                    if all_comb[i][j] in local_groups[m]:
                        local_group_id = m
                        break
                #print("   local_group_id = ", local_group_id)
                if local_parities[local_group_id] not in comb_list:
                    k_terms = k_terms - 1
                    comb_list.append(local_parities[local_group_id])
            if 'r' in all_comb[i][j]:
                r_terms = r_terms - 1
        #print("   ", k_terms, r_terms)
        if k_terms <= r_terms:
            recoverable = recoverable + 1
            #print(list(all_comb[i]), "-R")
        #else:
            #print(list(all_comb[i]), "-N")
        #print("   recoverable = ", recoverable)
        
    return (recoverable/len(all_comb))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse LRC configs')
    parser.add_argument('-k', type=int, help='Number of data blocks', default=6)
    parser.add_argument('-l', type=int, help='Number of local parities', default=2)
    parser.add_argument('-r', type=int, help='Number of global parities', default=2)
    parser.add_argument('-R', type=int, help='Number of failures', default=4)
    
    args = parser.parse_args()
    k = args.k
    l = args.l
    r = args.r
    R = args.R
    
    recoverability = calculate_recoverability(k, l, r, R)
    print("recoverability =", recoverability*100, "%")
