def net_raid_pdl(state):
    prob = 0
    failed_disks = state.get_failed_disks()
    for diskId in failed_disks:
        disk = state.disks[diskId]
        failed_disks_per_stripeset = state.get_failed_disks_per_stripeset(disk.stripesetId)
        if len(failed_disks_per_stripeset) > state.sys.top_m:
            prob = 1
            return prob
    return prob