#How to run this code ? "python3 lrc_decode.py -k=6 -l=2 -r=2 -R=4"
#where k=number of data blocks, l=number of local parities
#r=number of global parities, R=number of failures

from math import comb
import itertools
import argparse

#function to calculate recoverability
def calculate_recoverability(k, l, r, R):
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

    
    #generate all R failure combinations from whole set of data and parity blocks
    all_comb = list(itertools.combinations(data_and_parity, R))
    
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
    
    #start the recoverability calculation
    for i in range(len(all_comb)):
        k_terms = 0
        l_terms = l
        r_terms = r
        comb_list = list(all_comb[i])

        
        #find number of data blocks in a failure combo/pattern
        for j in range(len(all_comb[i])):
            if 'k' in comb_list[j]:
                k_terms = k_terms + 1

        
        #check if failed data block can be swapped with its local group parity
        #calculate the number of failed data blocks and available global parity blocks
        for j in range(len(all_comb[i])):
            if 'k' in all_comb[i][j]:
                for m in range(len(local_groups)):
                    if all_comb[i][j] in local_groups[m]:
                        local_group_id = m
                        break
                if local_parities[local_group_id] not in comb_list:
                    k_terms = k_terms - 1
                    comb_list.append(local_parities[local_group_id])
            if 'r' in all_comb[i][j]:
                r_terms = r_terms - 1
        if k_terms <= r_terms:
            recoverable = recoverable + 1
        
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
