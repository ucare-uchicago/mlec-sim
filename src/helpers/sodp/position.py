import numpy as np
import math

#TODO: check_accumulative_sum can probably be optimized through DP

# Convert from stripe->disk relationship to disk->stripesets relationship
def invert_stripeset_disk(stripesets):
    disks_to_stripes_map = {}
    disks_to_stripesets_map = {}
    for ss_idx in range(len(stripesets)):
        for diskId in stripesets[ss_idx]:
            curr_list = disks_to_stripes_map.get(diskId, [])
            curr_list.append(ss_idx)
            disks_to_stripes_map[diskId] = curr_list
            
            curr_list = disks_to_stripesets_map.get(diskId, set())
            curr_list.add(tuple(stripesets[ss_idx]))
            disks_to_stripesets_map[diskId] = curr_list
            
    return disks_to_stripes_map, disks_to_stripesets_map
        

def gsodp_stripeset_layout(num_disk_per_server, stripe_width, spool_id):
    # print("Generating stripeset layout for num_disk {}, stripe width {}".format(num_disk_per_server, stripe_width))
    base_dist = generate_base_dist(num_disk_per_server, stripe_width)
    # print(base_dist)
    base_stripeset = [0]
    for dist in base_dist:
        base_stripeset.append((base_stripeset[-1] + dist) % num_disk_per_server)
    stripesets = [base_stripeset]
    # print("Base distance is {}".format(base_dist))
    # print("Base stripeset is {}".format(base_stripeset))
    
    curr_stripeset = base_stripeset.copy()
    
    while True:
        new_stripeset = [(x + 1) % (num_disk_per_server) for x in curr_stripeset]
        #print("Current stripeset {}".format(new_stripeset))
        #print("Generated stripeset {}".format(stripesets))
        if new_stripeset == stripesets[0]:
            break
        # elif not overlap_check(stripesets, new_stripeset):
        else:
            stripesets.append(new_stripeset)
        
        curr_stripeset = new_stripeset
    
    for set in stripesets:
        for i in range(len(set)):
            set[i] += spool_id * len(stripesets)
    
    return stripesets

def generate_base_dist(num_disk_per_server, stripe_width):
    start = 2

    # return [2,3,1]

    # We need to test different start
    while start < stripe_width:
        dist = [1]
        curr = start
        
        while len(dist) != (stripe_width - 1):
            for insert_idx in range(0, len(dist)+1):
                #print("Looping 0")
                # Insert at insert_idx
                dist_copy = dist.copy()
                dist_copy.insert(insert_idx, curr)
                
                # Check validity
                if check_accumulative_sum(num_disk_per_server, dist_copy):
                    dist = dist_copy
                    break
            
            curr += 1
            #print("Looping 1")
        
        if len(dist) == stripe_width - 1:
            return dist
        elif len(dist) == stripe_width - 1:
            # If this combination itself results in overlapping disk within the stripe
            print("Enough dist, but adds up to stripewidth")
            dist = dist[:-1]
            curr += 1
            
        
        # However we need to check whether curr is already bigger than stripe_wdith
        if curr > stripe_width:
            # This means we run out of things to insert and we have not yet filled required distances
            # Move to next start
            start += 1
        
        #print("Looping 2")
        print("Starting on {}".format(start))
    
    raise Exception("?? Should not get here")

# Check whether the given stripset is valid
def check_accumulative_sum(num_disk_per_server, curr_distances):
    dist_set = set(curr_distances)
    # We loop through all the possible combinations to check whether the accumulative sum is equal to the index
    for cum_len in reversed(range(2, len(curr_distances) + 1)):
        #print("Checking cum_len {}".format(cum_len))
        # Starting at different index
        for i in range(0, len(curr_distances) - cum_len + 1):
            #print("Checking len {} starting at idx {}".format(cum_len, i))
            # Actually looping through the cum sum index
            cum_sum = 0
            
            for j in range(i, i+cum_len):
                cum_sum += curr_distances[j]
                
            #print("Summing from {} to {} yield {}".format(i, i+cum_len-1, cum_sum))
            
            if cum_sum in dist_set or cum_sum == num_disk_per_server:
                return False
            
            dist_set.add(cum_sum)

    # print("dist_set: {}".format(dist_set))
    return True

def overlap_check(stripesets, new_stripeset):
    for ss in stripesets:
        left = set(ss)
        right = set(new_stripeset)
        
        intersection = left.intersection(right)
        if len(intersection) > 1:
            #print("Overlap more than 1 - {}!".format(intersection))
            #print("Left with {}".format(left))
            #print("Right with {}".format(right))
            return True
        
    return False

def check_gsodp_stripset_correct(stripesets):
    for i in range(0, len(stripesets)):
        if overlap_check(stripesets[:i] + stripesets[i+1:], stripesets[i]):
            return False
    
    return True


    



def illustrate_sodp():
    n = 50
    k = 8
    p = 2
    s = k + p
    stripeset = gsodp_stripeset_layout(n, s, 0)
    print(stripeset)
    # print(len(stripeset))
    # #print(gsodp_stripeset_layout(16, 6))r
    # print(check_gsodp_stripset_correct(stripeset))
    
    # disk_to_stripeset = list(invert_stripeset_disk(stripeset).values())
    # print(disk_to_stripeset.pop(0))

    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    import random
    
    figure, axes = plt.subplots()
    x_range = [-0.5,len(stripeset)+2.5]
    y_range = [-1,n+1]
    axes.set_xlim(x_range)
    axes.set_ylim(y_range)


    plt.gca().invert_yaxis()


    for i in range(n):
        plt.text(i+0.1, -0.4, 'D'+str(i+1), font={'size': 20})

    colors = ['darkgreen', 'lightgreen', 'darkblue', 'lightblue', 'orange', 'brown', 'purple', 'pink']


    axes.add_patch(patches.Rectangle((-0.01, -0.01), n+0.02, 1.01, facecolor='white', edgecolor='black', alpha=1))
    plt.arrow(n+0.1, 0.5, 0.5, 0, head_width=0.2, head_length=0.2, length_includes_head=True, color='black', joinstyle='miter')
    plt.text(n+0.8, 0.6, 'Base Stripe', font={'size': 16}, color='black')


    plt.arrow(2, 1.5, 0.5, 0, head_width=0.2, head_length=0.2, length_includes_head=True, color='black', joinstyle='miter')
    plt.text(2.6, 1.65, 'Shift to right', font={'size': 16}, color='black')

    for i, stripe in enumerate(stripeset):
        # if count == 0:
            # axes.plot(racks, disks,marker='o',ms=radius,mfc=dl_color,mec=dl_color, mew=0.1)
        # color = (random.random(), random.random(), random.random())

        color = colors[i]

        y = i
        if y != 0:
            y += 1
        
        plt.text(-0.7, y+0.7, 'S'+str(i+1), font={'size': 16}, color='black')
        for j in range(n):
            if j in stripeset[i]:
                axes.add_patch(patches.Rectangle((j+0.03, y+0.03), 1-0.06, 1-0.06, facecolor=color, antialiased=False))
    

    

    plt.axis('off')
    plt.title('SODP (2+2) for 8 disks\n', font={'size': 20})
    plt.savefig('sodp.png',  bbox_inches='tight')


    for i, stripe in enumerate(stripeset):
        y = i
        if y != 0:
            y += 1
        for j in (0,1,2):
            if j in stripeset[i]:
                plt.text(j+0.2, y+0.85, 'X', font={'size': 24}, color='red')

    for j in (0,1,2):
        plt.text(j+0.2, -0.25, 'X', font={'size': 24}, color='red')
    
    plt.savefig('sodp-fail.png',  bbox_inches='tight')


if __name__ == "__main__":
    # #print(check_accumulative_sum([2,3,1]))
    # #print(generate_base_dist(6))
    illustrate_sodp()