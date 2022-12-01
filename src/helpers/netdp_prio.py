import numpy as np
import logging

# Scenario
# - one failure
# - located on one rack
# - disks with priority of 1
def f1p1(r,B,n):
    return 1

# Scenario
# - two failures
# - located on the same rack
# - disks with priority of 1
def f2p1_same(r,B,n):
    return 1

# Scenario
# - two failures
# - located on distinct rack
# - disks with priority of 1
def f2p1_distinct(r,B,n):
    a = (B-1)/B
    b = (n-1)/(r-1)
    c = (r-n)/(r-1)
    
    return a*b+c

# Scenario
# - two failures
# - located on distinct rack
# - disks with priority of 2
def f2p2_distinct(r,B,n):
    return fnpn_distinct(r,B,n,2)

# Scenario
# - three failures
# - located on the same rack
# - disks with priority of 1
def f3p1_same(r,B,n):
    return 1

# Scenario
# - three failures
# - 1 failure on one rack, and 2 failures on another
# - disk on the rack with 1 failure
# - disk with priority of 1
def f3p1_rack_with_1(r,B,n):
    a = (n-1)/(r-1)
    b = (B-2)/(B)
    c = (r-n)/(r-1)
    
    return a*b+c

# Scenario
# - three failures
# - 1 failure on one rack, and 2 failures on another
# - disk on the rack with 1 failure
# - disk with priority of 2
def f3p2_rack_with_1(r,B,n):
    a = (n-1)/(r-1)
    b = 2/B
    
    return a*b

# Scenario
# - three failures
# - 1 failure on one rack, and 2 failures on another
# - disk on the rack with 2 failure
# - disk with priority of 1
def f3p1_rack_with_2(r,B,n):
    a = (n-1)/(r-1)
    b = (B-1)/B
    c = (r-n)/(r-1)
    
    return a*b+c

# Scenario
# - three failures
# - 1 failure on one rack, and 2 failures on another
# - disk on the rack with 2 failure
# - disk with priority of 2
def f3p2_rack_with_2(r,B,n):
    a = (n-1)/(r-1)
    b = 1/B
    
    return a*b

# Scenario
# - thee failures
# - located on 3 distinct racks
# - disk with priority of 1
def f3p1_distinct(r,B,n):
    base = (r-1)*(r-2)
    a = ((r-n)*(r-n-1))/base
    b1 = (2*(r-n)*(n-1))/base
    b2 = (B*(B-1))/(B**2)
    c1 = ((n-1)*(n-2))/base
    c2 = ((B-1)**2)/(B**2)
    
    return a + b1*b2 + c1*c2

# Scneario
# - three failures
# - located on 3 distinct racks
# - disk with priority of 2
def f3p2_distinct(r,B,n):
    base = (r-1)*(r-2)
    a1 = ((r-n)*(n-1))/base
    a2 = (2*B)/(B**2)
    b1 = ((n-1)*(n-2))/base
    b2 = (2*(B-1))/(B**2)
    
    return a1*a2 + b1*b2

# Scenario
# - 3 failures
# - located on 3 distinct racks
# - disks with priority of 3
def f3p3_distinct(r,B,n):
    return fnpn_distinct(r,B,n,3)
    
# Scenario
# - f failures
# - located on distinct racks
# - disks with priority of f
def fnpn_distinct(r,B,n,f):
    a = 1/(B**(f-1))
    prod_i = np.arange(1, f)
    b_num = np.prod(n-prod_i)
    b_denom = np.prod(r-prod_i)
    logging.info("a: %s, b_num: %s, b_denom: %s", a, b_num, b_denom)
    
    return a*(b_num/b_denom)

def two_failure(r, B, n, disk,failed_disk_per_rack, priority):
    logging.info("Calling two_failure()")
    if (priority == 1):
        logging.info("f2p1_same()")
        return f2p1_same(r,B,n)
    elif (priority == 2):
        # Check the priority of the disk
        prio = disk.priority
        if (prio == 1):
            logging.info("f2p1_distinct()")
            return f2p1_distinct(r,B,n)
        elif (prio == 2):
            logging.info("f2p2_distinct()")
            return f2p2_distinct(r,B,n)
    else:
        raise Exception("Unknown failure state")
    
def three_failure(r, B, n, disk,failed_disk_per_rack, priority):
    logging.info("Calling three_failure()")
    if (priority == 1):
        return f3p1_same(r,B,n)
    elif (priority == 2):
        prio = disk.priority
        rack = disk.rackId
        if (len(failed_disk_per_rack[rack]) == 1):
            # Priority calculation for rack with 1 failure
            if (prio == 1):
                logging.info("Calling f3p1_rack_with_1()")
                return f3p1_rack_with_1(r,B,n)
            elif (prio == 2):
                logging.info("Calling f3p2_rack_with_1()")
                return f3p2_rack_with_1(r,B,n)
            else:
                raise Exception("Unknown failure state")
        elif (len(failed_disk_per_rack[rack]) == 2):
            # Priority calculation for rack with 2 failures
            if (prio == 1):
                logging.info("Calling f3p1_rack_with_2()")
                return f3p1_rack_with_2(r,B,n)
            elif (prio == 2):
                logging.info("Calling f3p2_rack_with_2()")
                return f3p2_rack_with_2(r,B,n)
            else:
                raise Exception("Unknown failure state")
        else:
            raise Exception("Unkown failure state")
    elif (priority == 3):
        prio = disk.priority
        if (prio == 1):
            logging.info("Calling f3p1_distinct()")
            return f3p1_distinct(r,B,n)
        elif (prio == 2):
            logging.info("Calling f3p2_distinct()")
            return f3p2_distinct(r,B,n)
        elif (prio == 3):
            logging.info("Calling f3p3_distinct()")
            return f3p3_distinct(r,B,n)
        else:
            raise Exception("Unkonwn failure state")
    else:
        raise Exception("Unknown failure state")
        
    
def priority_percent(state, disk,failed_disk_per_rack, max_priority, priority):
    r = len(state.racks)
    B = len(state.disks)/len(state.racks)
    n = state.n
    
    # print(f"Calculating pp for r={r}, B={B}, n={n}, failed_racks={failed_racks}")
    # Determine how many failures are there in the system
    
    logging.info("(r,B,n,failed_racks,max_prio,prio): (%s,%s,%s,%s,%s,%s)", r, B, n, priority, max_priority, disk.priority)
    
    if (max_priority == 1):
        return f1p1(r,B,n)
    elif (max_priority == 2):
        return two_failure(r, B, n, disk, failed_disk_per_rack, priority)
    elif (max_priority == 3):
        return three_failure(r, B, n, disk, failed_disk_per_rack, priority)
    elif (max_priority == disk.priority):
        return fnpn_distinct(r,B,n,priority)
    else:
        raise Exception("Unknown failure state")