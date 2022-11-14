def flat_cluster_pdl(state):
    prob = 0
    for rackId in state.sys.racks:
        fail_per_rack = state.get_failed_disks_per_rack(rackId)
        stripesets_per_rack = state.sys.flat_cluster_rack_layout[rackId]
        for stripeset in stripesets_per_rack:
            fail_per_set = set(stripeset).intersection(set(fail_per_rack))
            if len(fail_per_set) > state.sys.m:
                prob = 1
                return prob
    return prob