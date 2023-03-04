import logging

def mlec_cluster_pdl(state):
    # prob = 0
    # for i in range(state.policy.num_diskgroup_spools):
    #     failed_diskgroups_per_spool = state.policy.get_failed_diskgroups_per_spool(i)
    #     if len(failed_diskgroups_per_spool) > state.sys.top_m:
    #         #logging.warn("System failure caused by diskgroup spool %s with failures %s", i, failed_diskgroups_per_spool)
    #         prob = 1
    # return prob
    for failedDiskgroups in state.policy.affected_mlec_groups.values():
        if failedDiskgroups > state.sys.top_m:
            return 1
    return 0