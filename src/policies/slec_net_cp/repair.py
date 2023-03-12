from __future__ import annotations
import typing
import logging

if typing.TYPE_CHECKING:
    from state import State

from constants.Components import Components
from components.disk import Disk
from components.rack import Rack

from heapq import heappush, heapify

def slec_net_cp_repair(slec_net_cp, repair_queue):
    repair_queue.clear()
    for rackgroupId in slec_net_cp.affected_rackgroups:
        for spoolId in slec_net_cp.rackgroups[rackgroupId].affected_spools:
            spool = slec_net_cp.spools[spoolId]
            for diskId in spool.failed_disks:
                disk = slec_net_cp.disks[diskId]
                heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
            