from __future__ import annotations
import typing 
import logging

from heapq import heappush
from components.disk import Disk
from components.spool import Spool

# def mlec_repair(diskId, rackId, state):
#     return (state.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId)

# update the repair event queue
def mlec_repair(mlec_c_c, repair_queue):
    # logging.info('affected spools: {}'.format(mlec_c_c.affected_spools))
    for spoolId in mlec_c_c.affected_spools:
        spool = mlec_c_c.spools[spoolId]
        if len(spool.failed_disks) <= mlec_c_c.sys.m:
            for diskId in spool.failed_disks:
                heappush(repair_queue, (mlec_c_c.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId))
        else:
            heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))