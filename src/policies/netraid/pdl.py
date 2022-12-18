from __future__ import annotations
import typing
import logging
import time
if typing.TYPE_CHECKING:
    from state import State

def net_raid_pdl(state: State):
    prob = 0
    start = time.time()
    failed_disks = state.get_failed_disks()
    for diskId in failed_disks:
        disk = state.disks[diskId]
        failed_disks_per_stripeset = state.get_failed_disks_per_stripeset(disk.stripesetId)
        if len(failed_disks_per_stripeset) > state.sys.top_m:
            # logging.warn("System failure caused by stripe %s with failures %s", disk.stripesetId, failed_disks_per_stripeset)
            prob = 1
            # print("NETRAID check_pdl took " + str((time.time() - start) * 1000) + " ms")
            return prob
    # print("NETRAID check_pdl took " + str((time.time() - start) * 1000) + " ms")
    return prob