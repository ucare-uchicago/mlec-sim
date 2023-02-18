import random

def gen_failure_burst(disks_per_rack, num_total_racks, num_fail_disks):
    failures = {}
    for i in range(num_total_racks):
        failures[i] = 0
    num_disks = disks_per_rack * num_total_racks

    # we randomly sample num_fail_disks disk IDs
    diskIds = random.sample(range(num_disks), num_fail_disks)

    for diskId in diskIds:
        failures[diskId//disks_per_rack] += 1
    
    count = 0
    for i in range(num_total_racks):
        if failures[i] > 0:
            count += 1
    return count


for num_fail_disks in range(1, 26):
    iters = 100000
    counts = {}
    for i in range(num_fail_disks):
        counts[i+1] = 0
    for i in range(iters):
        count = gen_failure_burst(800, 40, num_fail_disks)
        counts[count] += 1

    with open("output.txt", "a") as f:
        for i in range(num_fail_disks):
            f.write("{} {} {}\n".format(num_fail_disks, i+1, counts[i+1]/iters))