from components.disk import Disk
from state import State
from system import System
from helpers.netdp_prio import stripe_fail_cases, stripe_total_cases
from helpers.netdp_prio import priority_percent_helper, two_failure

if __name__ == "__main__":
    # imagine we have 50 racks and 2 disk failures
    # test 8+2
    n = 10
    num_disks_per_rack = 1
    failures_per_other_affected_rack_list = [1]
    
    num_racks = 50
    r = 50 # len(state.racks), num_rack
    B = 1 #len(state.disks)/len(state.racks), num_disk_per_rack
    n = 10 #state.n, stripe_width
    
    disk = Disk(0, 20, 0)
    disk.priority = 2
    
    for priority in range(1,3):
        print('\npriority{}:'.format(priority))
        print('using generialized function....')
        priority_cases = stripe_fail_cases(n-1, priority-1, num_disks_per_rack, failures_per_other_affected_rack_list, 
                                            num_racks - 1 - len(failures_per_other_affected_rack_list))
        total_cases = stripe_total_cases(n-1, num_racks-1, num_disks_per_rack)
        
        print(priority_cases)
        print(total_cases)
        print(priority_cases / total_cases)

        print()
        print('using hardcoded function...')
    
    for max_priority in range(1, 4):
        for stripe_failures in range(1, 4):
            
            failed_drives_per_rack = range()
            priority_percent_helper(r,B,n,disk, fail)