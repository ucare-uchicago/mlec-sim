from __future__ import annotations
import typing 
if typing.TYPE_CHECKING:
    from state import State

from heapq import heappush
from constants.Components import Components
from components.disk import Disk
from components.diskgroup import Diskgroup

# def mlec_repair(diskId, rackId, state):
#     return (state.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId)


# update the repair event queue
def mlec_repair(diskgroups, failed_diskgroups, state: State, repair_queue):
    for diskgroupId in failed_diskgroups:
        if diskgroupId not in state.simulation.delay_repair_queue[Components.DISKGROUP]:
            heappush(repair_queue, (diskgroups[diskgroupId].estimate_repair_time, Diskgroup.EVENT_REPAIR, diskgroupId))
    
    for diskId in state.get_failed_disks():
        diskgroupId = diskId // state.sys.n
        if diskgroups[diskgroupId].state == Diskgroup.STATE_NORMAL and diskId not in state.simulation.delay_repair_queue[Components.DISK]:
            heappush(repair_queue, (state.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId))