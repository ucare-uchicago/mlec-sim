from __future__ import annotations
import typing
import logging

if typing.TYPE_CHECKING:
    from state import State

from constants.Components import Components
from components.disk import Disk
from components.rack import Rack

from heapq import heappush, heapify

def netraid_repair(state: State, repair_queue):
    netraid = state.policy
    for spoolId in netraid.affected_spools:
        spool = netraid.spools[spoolId]
        for diskId in spool.failed_disks:
            disk = netraid.disks[diskId]

            repair_time = disk.repair_time[0]
            #-----------------------------------------------------
            estimate_time = disk.repair_start_time
            estimate_time  += repair_time
            # Generate repair event
            heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
            