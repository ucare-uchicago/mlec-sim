import logging

def mlec_cluster_pdl(state):
    prob = 0
    for i in range(state.policy.num_diskgroup_stripesets):
        failed_diskgroups_per_stripeset = state.policy.get_failed_diskgroups_per_stripeset(i)
        if len(failed_diskgroups_per_stripeset) > state.sys.top_m:
            logging.warn("System failure caused by diskgroup stripeset %s with failures %s", i, failed_diskgroups_per_stripeset)
            prob = 1
    return prob