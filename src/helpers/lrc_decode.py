#How to run this code ? "python3 lrc_decode.py -k=6 -l=2 -r=2 -R=4"
#where k=number of data blocks, l=number of local parities
#r=number of global parities, R=number of failures

from math import comb
import itertools
import argparse
import time
import random
from concurrent.futures import ProcessPoolExecutor
import asyncio

# A brainless simple wrapper that waits on all the future in the list to complete
# and return the results
def wait_futures(futures):
    res = []
    # print("Job to be waited on " + str(len(futures)), flush=True)
    # We loop over a copy of the futures to avoid concurrent modification
    last_updated = len(futures)
    while len(futures) != 0:
        for future in list(futures):
            if future.done():
                try:
                    # print("Job result " + str(future.result()))
                    res.append(future.result())
                except asyncio.CancelledError:
                    # ignore
                    dummpy = -1
                futures.remove(future)
        if (len(futures) != last_updated):
            # print("Jobs remaining: " + str(len(futures)))
            last_updated = len(futures)
    
    return res

#function to calculate recoverability
def calculate_recoverability_random(k, l, r, R, iters=1000000):
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

    # print(data_and_parity)

    
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
    
    # do 100000 random simulations
    iters=iters
    #start the recoverability calculation
    for i in range(iters):
        k_terms = 0
        l_terms = l
        r_terms = r
        # generate random combination
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
        
    return (recoverable/iters)

#function to calculate recoverability
def calculate_recoverability_brute(k, l, r, R):
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
    #print(local_groups)
    
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


def compute_decode_recoverability(k, l, r, R, use_random=True, iters=1000000):
    if use_random:
        epochs = 256
        
        executor = ProcessPoolExecutor(epochs)
        futures = []
        for epoch in range(0, epochs):
            futures.append(executor.submit(calculate_recoverability_random, k, l, r, R, iters))
        ress = wait_futures(futures)
        executor.shutdown()
        recoverability = sum(ress) / len(ress)
    else:
        recoverability=calculate_recoverability_brute(k, l, r, R)
    return recoverability

if __name__ == "__main__":
    start_time = time.time()
    parser = argparse.ArgumentParser(description='Parse LRC configs')
    parser.add_argument('-k', type=int, help='Number of data blocks', default=6)
    parser.add_argument('-l', type=int, help='Number of local parities', default=2)
    parser.add_argument('-r', type=int, help='Number of global parities', default=2)
    parser.add_argument('-R', type=int, help='Number of failures', default=4)
    parser.add_argument('-iters', type=int, help='Number of random iterations', default=1000000)
    parser.add_argument('--random', action='store_true', help='Random sampling')
    
    args = parser.parse_args()
    k = args.k
    l = args.l
    r = args.r
    R = args.R
    use_random = args.random
    iters = args.iters
    
    recoverability = compute_decode_recoverability(k, l, r, R, use_random, iters)
    
    print("recoverability =", recoverability*100, "%")

    print("execution time =", time.time()-start_time)