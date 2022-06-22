import numpy as np
import gc


# Count the number of failures
def system_failures_count(system, time):
    return len(system[system>time])

# This generate the config maps of different stripes
def gen_config_map(total_shards, total_drives):
    generated_set = np.array([])
    config_map = []

    while len(generated_set) != total_drives:
        gen = np.random.randint(low=0, high=total_drives, size=total_shards)
        config_map.append(gen)
        generated_set = np.unique(np.append(generated_set, gen))
    
    gc.collect()
    return config_map

# Get the stripes that contains the failed diskId
def stripes_containing_disk(diskId, configMap):
    stripes = []
    for stripe in configMap:
        if diskId in stripe:
            stripes.append(stripe)
    
    return stripes

# Get the failure count of a stripe given all failed diskIds
def stripe_fails(stripe, all_fails):
    return len(np.intersect1d(stripe, all_fails))