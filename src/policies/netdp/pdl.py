import logging

def check_pdl(state):
    # prob = 0
    
    # # If there are more than m racks that contains failures
    # #  there has to be a stripe that has m failed chunks
    # #  therefore the system fails
    # rack_with_failures = state.get_failed_disks_each_rack()[1]
    
    
    # if (rack_with_failures > self.sys.m):
    #     logging.info("SYS FAILURE WITH RACK FAILURE: " + str(state.get_failed_disks_each_rack()[0]))
    #     logging.info("SYSTEM FAILS!")
    #     logging.info(state.get_failed_disks_each_rack()[0])
    #     prob = 1

    # return prob
    
    # Note on the above commented out section:
    #   Previously, the number of racks containing failure was used to check whether the system has failed
    #   however, this approach is incorrect because we could have already repaired the critical chunks
    #   that puts the system on the critical path. Two racks containing failures does not necessarily
    #   mean that priority 2 stripes exist. We need to check disk's priority to make sure.
    prob = 0
    for diskId in state.disks:
        #print "diskId", diskId, "priority",state.disks[diskId].priority
        if state.disks[diskId].priority > state.sys.m:
            logging.info("SYS FAILURE WITH RACK FAILURE: " + str(state.get_failed_disks_each_rack()[0]))
            logging.info("Cause of failure: diskId %s with priority %s", diskId, state.disks[diskId].priority)
            logging.info(state.get_failed_disks_each_rack()[0])
            prob = 1
            return prob
    return prob