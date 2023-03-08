def flat_cluster_pdl(state):
    for raidgroupId in state.failures_per_raidgroup:
        if state.failures_per_raidgroup[raidgroupId] > state.sys.m:
            return 1        # data loss
    return 0