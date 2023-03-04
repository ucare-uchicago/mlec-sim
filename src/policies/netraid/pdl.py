from __future__ import annotations
import typing
import logging
import time
if typing.TYPE_CHECKING:
    from state import State
    from policies.netraid.netraid import NetRAID

def net_raid_pdl(netraid: NetRAID, state: State):
    if netraid.sys_failed:
        return 1
    else:
        return 0
    
def old(netraid: NetRAID, state: State):
    prob = 0
    start = time.time()
    failed_disks = state.get_failed_disks()
    # print("Number of failed disks: " + str(len(failed_disks)))
    for diskId in failed_disks:
        disk = state.disks[diskId]
        # start2 = time.time()
        failed_disks_per_spool = state.get_failed_disks_per_spool(disk.spoolId)
        # print("Getting failed disks took " + str((time.time() - start2) * 1000) + " ms")
        if len(failed_disks_per_spool) > state.sys.top_m:
            # logging.warn("System failure caused by stripe %s with failures %s", disk.spoolId, failed_disks_per_spool)
            prob = 1
            # print("NETRAID check_pdl took " + str((time.time() - start) * 1000) + " ms")
            return prob
    # print("NETRAID check_pdl took " + str((time.time() - start) * 1000) + " ms")
    return prob