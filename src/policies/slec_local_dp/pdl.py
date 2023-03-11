def slec_local_dp_pdl(state):
    prob = 0
    for diskId in state.disks:
        #print "diskId", diskId, "priority",state.disks[diskId].priority
        if state.disks[diskId].priority > state.sys.m:
            prob = 1
            return prob
    return prob